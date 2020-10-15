#include "Expression.h"
namespace k2 {
namespace dto {
namespace expression {

struct SchematizedValue {
    SchematizedValue(Value& v, SKVRecord& rec) : val(v), rec(rec), type(v.type) {
        if (!val.fieldName.empty()) {
            for (size_t i = 0; i < rec.schema->fields.size(); ++i) {
                if (rec.schema->fields[i].name == val.fieldName) {
                    sfieldIndex = i;
                    nullLast = rec.schema->fields[i].nullLast;
                    type = rec.schema->fields[i].type;
                    break;
                }
            }
        }
    }
    Value& val;
    SKVRecord& rec;
    FieldType type = FieldType::NOT_KNOWN;
    bool nullLast = false;
    int sfieldIndex = -1;

    template <typename T>
    std::optional<std::tuple<bool, T>> get() {
        if (TToFieldType<T>() != type) {
            throw TypeMismatchException();
        }
        if (val.fieldName.empty()) {
            T result{};
            if (val.literal.read(val)) {
                return std::make_tuple(nullLast, std::move(result));
            }
            return std::nullopt;
        }
        auto&& opt = rec.deserializeField<T>(sfieldIndex);
        if (!opt) {
            return std::nullopt;
        }
        return std::make_tuple(nullLast, std::move(opt).value());
    }
};

template <typename T1, typename T2, typename = void>
struct is_comparable : std::false_type {};

template <typename T1, typename T2>
struct is_comparable<T1, T2,
                     decltype(
                         (std::declval<T1>() == std::declval<T2>()) &&
                             (std::declval<T1>() < std::declval<T2>()) &&
                             (std::declval<T1>() > std::declval<T2>()),
                         void())>
    : std::true_type {};

template <typename T1, typename T2>
std::enable_if_t<is_comparable<T1, T2>::value, int>
compare(T1& a, T2& b) {
    if (std::forward<T1>(a) == std::forward<T2>(b)) return 0;
    if (std::forward<T1>(a) > std::forward<T2>(b)) return 1;
    return -1;
}

template <typename T1, typename T2>
std::enable_if_t<!is_comparable<T1, T2>::value, int>
compare(T1&, T2&) {
    throw TypeMismatchException();
}


template <typename T1, typename T2>
std::enable_if_t<!is_comparable<T1, T2>::value, int>
compareOptionals(std::optional<std::tuple<bool, T1>>&, std::optional<std::tuple<bool,T2>>&) {
    throw TypeMismatchException();
}

template <typename T1, typename T2>
std::enable_if_t<is_comparable<T1, T2>::value, int>
compareOptionals(std::optional<std::tuple<bool, T1>>& a, std::optional<std::tuple<bool,T2>>& b) {
    if (!a && !b) {
        // NULLs of compatible types compare as equal
        return 0;
    }

    // one of the operands may be null. Compare based on nullLast flag, regardless of type
    if (!a) {
        return std::get<0>(*b) ? 1 : -1;
    }
    else if (!b) {
        return std::get<0>(*a) ? -1 : 1;
    }

    // operands are non-null and of same type. Compare their values
    if (std::get<1>(*a) == std::get<1>(*b)) {
        return -1;
    }
    if (std::get<1>(*a) > std::get<1>(*b)) {
        return 1;
    }
    return -1;
}

#define CAST_APPLY_VALUE(func, a, ...)              \
    do {                                            \
        switch (a.type) {                           \
            case FieldType::NOT_KNOWN: {            \
                throw TypeMismatchException();      \
            } break;                                \
            case FieldType::STRING: {               \
                func<k2::String>(a, __VA_ARGS__);   \
            } break;                                \
            case FieldType::INT16T: {               \
                func<int16_t>(a, __VA_ARGS__);      \
            } break;                                \
            case FieldType::INT32T: {               \
                func<int32_t>(a, __VA_ARGS__);      \
            } break;                                \
            case FieldType::INT64T: {               \
                func<int64_t>(a, __VA_ARGS__);      \
            } break;                                \
            case FieldType::FLOAT: {                \
                func<float>(a, __VA_ARGS__);        \
            } break;                                \
            case FieldType::DOUBLE: {               \
                func<double>(a, __VA_ARGS__);       \
            } break;                                \
            default:                                \
                throw InvalidExpressionException(); \
        }                                           \
    } while (0);

template <typename B_TYPE, typename A>
void _innerCompareHelper(SchematizedValue& b, A& a_opt, bool& result) {
    auto b_opt = b.get<B_TYPE>();
    result = compareOptionals(a_opt, b_opt);
}

template <typename A_TYPE>
void _outerCompareHelper(SchematizedValue& a, SchematizedValue& b, bool& result) {
    auto a_opt = a.get<A_TYPE>();
    // This relies on partial template deduction of the last template argument of innerHelper
    CAST_APPLY_VALUE(_innerCompareHelper, b, a_opt, result);
}

bool _compareSValues(SchematizedValue& a, SchematizedValue& b) {
    bool result = false;
    CAST_APPLY_VALUE(_outerCompareHelper, a, b, result);
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
        case Operation::IS_TYPE: {
            return IS_TYPE_handler(rec);
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
        default:
            throw InvalidExpressionException();
    }
}

bool Expression::EQ_handler(SKVRecord& rec) {
    // this op evaluates exactly two leafs only. It cannot be composed with other children
    if (valueChildren.size() != 2 || expressionChildren.size() > 0) {
        throw InvalidExpressionException();
    }
    SchematizedValue aVal(valueChildren[0], rec);
    SchematizedValue bVal(valueChildren[1], rec);
    return _compareSValues(aVal, bVal) == 0;
}

bool Expression::GT_handler(SKVRecord& rec) {
    (void)rec;
    return false;
}

bool Expression::GTE_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::LT_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::LTE_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::IS_NULL_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::IS_TYPE_handler(SKVRecord& rec) {
    (void)rec;
    return false;
}

bool Expression::STARTS_WITH_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::CONTAINS_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::ENDS_WITH_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::AND_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::OR_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::XOR_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

bool Expression::NOT_handler(SKVRecord& rec) {
    (void) rec;
    return false;
}

} // ns expression
} // dto
} // k2
