/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include "Expression.h"
namespace k2 {
namespace dto {
namespace expression {

template <class T> void setPayloadValue(Value& target,  const nlohmann::json& jsonval) {
    T val;

    if constexpr  (std::is_same_v<T, std::decimal::decimal64>
        || std::is_same_v<T, std::decimal::decimal128>) {
        // No support to deserialize json to std::decimlal yet
        throw k2::dto::TypeMismatchException("decimal type not supported with JSON interface");
    } else {
        jsonval.get_to(val);
        target.literal.write(val);
    }
}

// Specialization to to avoid hex decoding of k2::String
template <>
void setPayloadValue<k2::String>(Value& target,  const nlohmann::json& jsonval) {
    std::string val;
    // Use std::string to decode k2::String to avoid hex decoding
    jsonval.get_to(val);
    target.literal.write(val);    
}

void from_json(const nlohmann::json& j, Value& v) {
    v.fieldName = j.at("fieldName").get<k2::String>();
    if (v.fieldName.length() > 0) {
        // No need to read type and literal from json as it represends a record field
        return;
    }
    v.type = j.at("type").get<dto::FieldType>();
    K2_DTO_CAST_APPLY_FIELD_VALUE(setPayloadValue, v, j.at("literal"));
}

// This class wraps a given Value together with particular SKVRecord (and its schema). It is used to perform expression
// operations according to schema, provided literals, and found reference values.
struct SchematizedValue {
    SchematizedValue(Value& v, SKVRecord& rec) : val(v), rec(rec), type(v.type) {
        K2ASSERT(log::dto, rec.schema, "Record must have a schema");
        if (val.isReference()) {
            for (size_t i = 0; i < rec.schema->fields.size(); ++i) {
                if (rec.schema->fields[i].name == val.fieldName) {
                    sfieldIndex = i;
                    nullLast = rec.schema->fields[i].nullLast;
                    type = rec.schema->fields[i].type;
                    break;
                }
            }
            // If a fieldName has been provided(meaning this is a reference value), and we can't find it in the incoming
            // schema, then we can't
            if (type == FieldType::NOT_KNOWN) {
                throw NoFieldFoundException("unable to create reference with FieldType::NOT_KNOWN");
            }
        }
    }
    Value& val;
    SKVRecord& rec;
    FieldType type = FieldType::NOT_KNOWN;
    bool nullLast = false;
    int sfieldIndex = -1;

    // Extracts the nullLast flag and an optional of a given type for this SchematizedValue.
    template <typename T>
    std::tuple<bool, std::optional<T>> get() {
        if (TToFieldType<T>() != type) {
            throw TypeMismatchException(fmt::format("bad type in schematized value get: have {}, got {}", type, TToFieldType<T>()));
        }
        if (!val.isReference()) {
            val.literal.seek(0);
            T result{};
            if (val.literal.read(result)) {
                return std::make_tuple(nullLast, std::move(result));
            }
            throw DeserializationError(fmt::format("Unable to deserialize value literal of type {}", TToFieldType<T>()));
        }
        return std::make_tuple(nullLast, rec.deserializeField<T>(sfieldIndex));
    }
};

// Template specialization for types which are not comparable
template <typename T1, typename T2, typename = void>
struct is_comparable : std::false_type {};

// Template specialization for types which are comparable (that is there is a defined operators <, ==, and >)
template <typename T1, typename T2>
struct is_comparable<T1, T2,
                     decltype(
                         (std::declval<T1>() == std::declval<T2>()) &&
                         (std::declval<T1>() < std::declval<T2>()) &&
                         (std::declval<T1>() <= std::declval<T2>()) &&
                         (std::declval<T1>() >= std::declval<T2>()) &&
                         (std::declval<T1>() > std::declval<T2>()),
                         void())>
    : std::true_type {};

// template specialization comparing two optionals of non-comparable types
template <typename T1, typename T2>
std::enable_if_t<!is_comparable<T1, T2>::value, int>
compareOptionals(std::tuple<bool, std::optional<T1>>&, std::tuple<bool, std::optional<T2>>&) {
    throw TypeMismatchException(fmt::format("non-comparable types: {}, {}", TToFieldType<T1>(), TToFieldType<T1>()));
}

// template specialization comparing two optionals of comparable types
template <typename T1, typename T2>
std::enable_if_t<is_comparable<T1, T2>::value, int>
compareOptionals(std::tuple<bool, std::optional<T1>>& a, std::tuple<bool, std::optional<T2>>& b) {
    auto& [a_nullLast, a_opt] = a;
    auto& [b_nullLast, b_opt] = b;

    if (!a_opt && !b_opt) {
        // NULLs of compatible types compare as equal
        return 0;
    }

    // one of the operands may be null. Compare based on nullLast flag, regardless of type
    if (!a_opt) {
        return b_nullLast ? 1 : -1;
    }
    else if (!b_opt) {
        return a_nullLast ? -1 : 1;
    }

    // operands are non-null and of same type. Compare their values
    if (*a_opt == *b_opt) {
        return 0;
    }
    if (*a_opt > *b_opt) {
        return 1;
    }
    return -1;
}



template <typename B_TYPE, typename A>
void _innerCompareHelper(SchematizedValue& b, A& a_opt, int& result) {
    auto b_opt = b.get<B_TYPE>();
    result = compareOptionals(a_opt, b_opt);
}

template <typename A_TYPE>
void _outerCompareHelper(SchematizedValue& a, SchematizedValue& b, int& result) {
    auto a_opt = a.get<A_TYPE>();
    // This relies on partial template deduction of the last template argument of innerHelper
    K2_DTO_CAST_APPLY_FIELD_VALUE(_innerCompareHelper, b, a_opt, result);
}

int _compareSValues(SchematizedValue& a, SchematizedValue& b) {
    int result = 0;
    K2_DTO_CAST_APPLY_FIELD_VALUE(_outerCompareHelper, a, b, result);
    return result;
}

void Expression::copyPayloads() {
    for (Value& value : valueChildren) {
        Payload copied = value.literal.copy();
        value.literal = std::move(copied);
    }

    for (Expression& exp : expressionChildren) {
        exp.copyPayloads();
    }
}

bool Expression::evaluate(SKVRecord& rec) {
    switch(op) {
        case Operation::EQ: {
            return EQ_handler(rec);
        }
        case Operation::GT: {
            return GT_handler(rec);
        }
        case Operation::GTE: {
            return GTE_handler(rec);
        }
        case Operation::LT: {
            return LT_handler(rec);
        }
        case Operation::LTE: {
            return LTE_handler(rec);
        }
        case Operation::IS_NULL: {
            return IS_NULL_handler(rec);
        }
        case Operation::IS_EXACT_TYPE: {
            return IS_EXACT_TYPE_handler(rec);
        }
        case Operation::STARTS_WITH: {
            return STARTS_WITH_handler(rec);
        }
        case Operation::CONTAINS: {
            return CONTAINS_handler(rec);
        }
        case Operation::ENDS_WITH: {
            return ENDS_WITH_handler(rec);
        }
        case Operation::AND: {
            return AND_handler(rec);
        }
        case Operation::OR: {
            return OR_handler(rec);
        }
        case Operation::XOR: {
            return XOR_handler(rec);
        }
        case Operation::NOT: {
            return NOT_handler(rec);
        }
        case Operation::UNKNOWN: {
            if (valueChildren.size() + expressionChildren.size() == 0) {
                // empty expression - allow it
                return true;
            }
            throw InvalidExpressionException(fmt::format("UNKNOWN operation {} in expression", op));
        }
        default:
            throw InvalidExpressionException(fmt::format("non-supported operation {} in expression", op));
    }
}

bool Expression::EQ_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression EQ must have exactly 2 value children(have {}) and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) == 0;
}

bool Expression::GT_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression GT must have exactly 2 value children(have {}) and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) > 0;
}

bool Expression::GTE_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression GTE must have exactly 2 value children(have {}) and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) >= 0;
}

bool Expression::LT_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression LT must have exactly 2 value children(have {}) and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) < 0;
}

bool Expression::LTE_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression LTE must have exactly 2 value children(have {}) and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) <= 0;
}

template<typename T>
void _isNullTypedHelper(SchematizedValue& sv, bool& result) {
    auto [nullLast, optVal] = sv.get<T>();
    result = !optVal.has_value();
}

bool Expression::IS_NULL_handler(SKVRecord& rec) {
    // this op evaluates exactly one reference leaf only. It cannot be composed with other children
    if (valueChildren.size() != 1 || expressionChildren.size() > 0 || !valueChildren[0].isReference()) {
        throw InvalidExpressionException(fmt::format("expression IS_NULL must have exactly 1 value literal children(have {}) and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }

    // There is just one value child and it is a reference.
    SchematizedValue ref(valueChildren[0], rec);

    bool result = false;
    K2_DTO_CAST_APPLY_FIELD_VALUE(_isNullTypedHelper, ref, result);
    return result;
}

bool Expression::IS_EXACT_TYPE_handler(SKVRecord& rec) {
    // this op evaluates a field reference againts a string literal(the type in question)
    if (valueChildren.size() != 2 || expressionChildren.size() > 0 ||
       !valueChildren[0].isReference() || valueChildren[1].isReference()) {
        throw InvalidExpressionException(fmt::format("expression IS_EXACT_TYPE must have exactly 2 value children, where child0 is a literal and child1 is a reference(have {}) and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }

    // There is just one value child and it is a reference.
    SchematizedValue ref(valueChildren[0], rec);
    SchematizedValue expTypeVal(valueChildren[1], rec);

    FieldType expected = std::get<1>(expTypeVal.get<FieldType>()).value();

    return expected == ref.type;
}

bool Expression::STARTS_WITH_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression STARTS_WITH must have exactly 2 value children and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    if (aVal.type != FieldType::STRING || bVal.type != FieldType::STRING) {
        throw TypeMismatchException(fmt::format("STARTS_WITH handler non-string fields: {}, {}", aVal.type, bVal.type));
    }
    auto aOpt = std::get<1>(aVal.get<String>());
    auto bOpt = std::get<1>(bVal.get<String>());
    if (!bOpt) return true; // all strings start with nothing
    if (!aOpt) return false; // empty strings do not start with anything

    if (aOpt->size() < bOpt->size()) return false; // B is bigger so A cannot start with B

    return ::memcmp(aOpt->c_str(), bOpt->c_str(), bOpt->size()) == 0;
}

bool Expression::CONTAINS_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression CONTAINS must have exactly 2 value children and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    if (aVal.type != FieldType::STRING || bVal.type != FieldType::STRING) {
        throw TypeMismatchException(fmt::format("CONTAINS handler non-string fields: {}, {}", aVal.type, bVal.type));
    }
    auto aOpt = std::get<1>(aVal.get<String>());
    auto bOpt = std::get<1>(bVal.get<String>());
    if (!bOpt) return true;   // all strings contain a null
    if (!aOpt) return false;  // a null string doesn't contain other strings

    return aOpt->find(*bOpt) != String::npos;
}

bool Expression::ENDS_WITH_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException(fmt::format("expression ENDS_WITH must have exactly 2 value children and no expression children(have {})", valueChildren.size(), expressionChildren.size()));
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    if (aVal.type != FieldType::STRING || bVal.type != FieldType::STRING) {
        throw TypeMismatchException(fmt::format("ENDS_WITH handler non-string fields: {}, {}", aVal.type, bVal.type));
    }
    auto aOpt = std::get<1>(aVal.get<String>());
    auto bOpt = std::get<1>(bVal.get<String>());
    if (!bOpt) return true;   // all strings end with nothing
    if (!aOpt) return false;  // null strings do not end with anything

    if (aOpt->size() < bOpt->size()) return false;  // B is bigger so A cannot start with B

    return ::memcmp(aOpt->c_str() + (aOpt->size() - bOpt->size()), bOpt->c_str(), bOpt->size()) == 0;
}

bool Expression::AND_handler(SKVRecord& rec) {
    // this op evaluates exactly two children only
    if (valueChildren.size() + expressionChildren.size() != 2) {
        throw InvalidExpressionException(fmt::format("expression AND must have exactly 2 total children(have {} value and {} expression)", valueChildren.size(), expressionChildren.size()));
    }
    // case 1: 2 values
    if (valueChildren.size() == 2) {
        SchematizedValue aVal(valueChildren[0], rec);
        SchematizedValue bVal(valueChildren[1], rec);
        if (aVal.type != FieldType::BOOL || bVal.type != FieldType::BOOL) {
            throw TypeMismatchException(fmt::format("AND handler 2 values non-bool fields: {}, {}", aVal.type, bVal.type));
        }
        auto [nullA, aOpt] = aVal.get<bool>();
        auto [nullB, bOpt] = bVal.get<bool>();
        return aOpt.has_value() && bOpt.has_value() && (*aOpt) && (*bOpt);
    }
    else if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException(fmt::format("AND handler single non-bool field: {}", aVal.type));
        }
        auto [nullLast, aOpt] = aVal.get<bool>();
        // make sure to always evaluate in order to trigger type exceptions if any
        auto expEval = expressionChildren[0].evaluate(rec);
        return aOpt.has_value() && (*aOpt) && expEval;
    }
    // always evaluate fully both children to trigger type exceptions if any
    auto expAeval = expressionChildren[0].evaluate(rec);
    auto expBeval = expressionChildren[1].evaluate(rec);
    return expAeval && expBeval;
}

bool Expression::OR_handler(SKVRecord& rec) {
    // this op evaluates exactly two children only
    if (valueChildren.size() + expressionChildren.size() != 2) {
        throw InvalidExpressionException(fmt::format("expression OR must have exactly 2 total children(have {} value and {} expression)", valueChildren.size(), expressionChildren.size()));
    }
    // case 1: 2 values
    if (valueChildren.size() == 2) {
        SchematizedValue aVal(valueChildren[0], rec);
        SchematizedValue bVal(valueChildren[1], rec);
        if (aVal.type != FieldType::BOOL || bVal.type != FieldType::BOOL) {
            throw TypeMismatchException(fmt::format("OR handler two non-bool fields: {}, {}", aVal.type, bVal.type));
        }
        auto [nullA, aOpt] = aVal.get<bool>();
        auto [nullB, bOpt] = bVal.get<bool>();
        return aOpt.has_value() && bOpt.has_value() && (*aOpt || *bOpt);
    } else if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException(fmt::format("OR handler single non-bool field: {}", aVal.type));
        }
        auto [nullLast, aOpt] = aVal.get<bool>();
        // make sure to always evaluate in order to trigger type exceptions if any
        auto eval = expressionChildren[0].evaluate(rec);
        return aOpt.has_value() && (*aOpt || eval);
    }

    // always evaluate fully both children to trigger type exceptions if any
    auto expAeval = expressionChildren[0].evaluate(rec);
    auto expBeval = expressionChildren[1].evaluate(rec);
    return expAeval || expBeval;
}

bool Expression::XOR_handler(SKVRecord& rec) {
    // this op evaluates exactly two children only
    if (valueChildren.size() + expressionChildren.size() != 2) {
        throw InvalidExpressionException(fmt::format("expression XOR must have exactly 2 total children(have {} value and {} expression)", valueChildren.size(), expressionChildren.size()));
    }
    // case 1: 2 values
    if (valueChildren.size() == 2) {
        SchematizedValue aVal(valueChildren[0], rec);
        SchematizedValue bVal(valueChildren[1], rec);
        if (aVal.type != FieldType::BOOL || bVal.type != FieldType::BOOL) {
            throw TypeMismatchException(fmt::format("XOR handler two non-bool fields: {}, {}", aVal.type, bVal.type));
        }
        auto [nullA, aOpt] = aVal.get<bool>();
        auto [nullB, bOpt] = bVal.get<bool>();
        return aOpt.has_value() && bOpt.has_value() && (*aOpt != *bOpt);
    } else if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException(fmt::format("XOR handler single non-bool field: {}", aVal.type));
        }
        auto [nullLast, aOpt] = aVal.get<bool>();
        // make sure to always evaluate in order to trigger type exceptions if any
        auto eval = expressionChildren[0].evaluate(rec);
        return aOpt.has_value() && (*aOpt != eval);
    }

    // always evaluate fully both children to trigger type exceptions if any
    auto expAeval = expressionChildren[0].evaluate(rec);
    auto expBeval = expressionChildren[1].evaluate(rec);
    return expAeval != expBeval;
}

bool Expression::NOT_handler(SKVRecord& rec) {
    // this op evaluates exactly 1 child only
    if (valueChildren.size() + expressionChildren.size() != 1) {
        throw InvalidExpressionException(fmt::format("expression NOT must have exactly 1 total children(have {} value and {} expression)", valueChildren.size(), expressionChildren.size()));
    }
    if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException(fmt::format("NOT handler single non-bool field: {}", aVal.type));
        }
        auto [nullA, aOpt] = aVal.get<bool>();

        // no value means "false". That means we need to return not(false), i.e. "true" if not set
        return !aOpt.has_value() || !(*aOpt);
    }

    return !expressionChildren[0].evaluate(rec);
}

} // ns expression
} // dto
} // k2
