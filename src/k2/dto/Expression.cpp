#include "Expression.h"
namespace k2 {
namespace dto {
namespace expression {

// This class wraps a given Value together with particular SKVRecord (and its schema). It is used to perform expression
// operations according to schema, provided literals, and found reference values.
struct SchematizedValue {
    SchematizedValue(Value& v, SKVRecord& rec) : val(v), rec(rec), type(v.type) {
        K2ASSERT(rec.schema, "Record must have a schema");
        if (!val.fieldName.empty()) {
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
                throw NoFieldFoundException();
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
            throw TypeMismatchException();
        }
        if (val.fieldName.empty()) {
            val.literal.seek(0);
            T result{};
            if (val.literal.read(result)) {
                return std::make_tuple(nullLast, std::move(result));
            }
            throw DeserializationError();
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
    throw TypeMismatchException();
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
            throw InvalidExpressionException();
        }
        default:
            throw InvalidExpressionException();
    }
}

bool Expression::EQ_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) == 0;
}

bool Expression::GT_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) > 0;
}

bool Expression::GTE_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) >= 0;
}

bool Expression::LT_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) < 0;
}

bool Expression::LTE_handler(SKVRecord& rec) {
    // this op evaluates exactly two values only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException();
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
        throw InvalidExpressionException();
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
        throw InvalidExpressionException();
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
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    if (aVal.type != FieldType::STRING || bVal.type != FieldType::STRING) {
        throw TypeMismatchException();
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
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    if (aVal.type != FieldType::STRING || bVal.type != FieldType::STRING) {
        throw TypeMismatchException();
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
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    if (aVal.type != FieldType::STRING || bVal.type != FieldType::STRING) {
        throw TypeMismatchException();
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
        throw InvalidExpressionException();
    }
    // case 1: 2 values
    if (valueChildren.size() == 2) {
        SchematizedValue aVal(valueChildren[0], rec);
        SchematizedValue bVal(valueChildren[1], rec);
        if (aVal.type != FieldType::BOOL || bVal.type != FieldType::BOOL) {
            throw TypeMismatchException();
        }
        auto [nullA, aOpt] = aVal.get<bool>();
        auto [nullB, bOpt] = bVal.get<bool>();
        return aOpt.has_value() && bOpt.has_value() && (*aOpt) && (*bOpt);
    }
    else if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException();
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
        throw InvalidExpressionException();
    }
    // case 1: 2 values
    if (valueChildren.size() == 2) {
        SchematizedValue aVal(valueChildren[0], rec);
        SchematizedValue bVal(valueChildren[1], rec);
        if (aVal.type != FieldType::BOOL || bVal.type != FieldType::BOOL) {
            throw TypeMismatchException();
        }
        auto [nullA, aOpt] = aVal.get<bool>();
        auto [nullB, bOpt] = bVal.get<bool>();
        return aOpt.has_value() && bOpt.has_value() && (*aOpt || *bOpt);
    } else if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException();
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
        throw InvalidExpressionException();
    }
    // case 1: 2 values
    if (valueChildren.size() == 2) {
        SchematizedValue aVal(valueChildren[0], rec);
        SchematizedValue bVal(valueChildren[1], rec);
        if (aVal.type != FieldType::BOOL || bVal.type != FieldType::BOOL) {
            throw TypeMismatchException();
        }
        auto [nullA, aOpt] = aVal.get<bool>();
        auto [nullB, bOpt] = bVal.get<bool>();
        return aOpt.has_value() && bOpt.has_value() && (*aOpt != *bOpt);
    } else if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException();
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
        throw InvalidExpressionException();
    }
    if (valueChildren.size() == 1) {
        SchematizedValue aVal(valueChildren[0], rec);
        if (aVal.type != FieldType::BOOL) {
            throw TypeMismatchException();
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
