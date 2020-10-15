#include <k2/transport/Payload.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <vector>
#include <functional>
#include "FieldTypes.h"
#include "SKVRecord.h"

namespace k2 {
namespace dto {
namespace expression {

struct InvalidExpressionException : public std::exception {
    virtual const char* what() const noexcept override{ return "InvalidExpression";}
};

struct TypeMismatchException : public std::exception {
    virtual const char* what() const noexcept override { return "TypeMismatch"; }
};

enum class Operation : uint8_t {
    EQ,           // value operands only
    GT,           // value operands only
    GTE,          // value operands only
    LT,           // value operands only
    LTE,          // value operands only
    IS_NULL,      // value operands only
    IS_TYPE,      // value operands only
    STARTS_WITH,  // value operands only
    CONTAINS,     // value operands only
    ENDS_WITH,    // value operands only
    AND,          // mixed value and/or nested expression operands
    OR,           // mixed value and/or nested expression operands
    XOR,          // mixed value and/or nested expression operands
    NOT,           // mixed value and/or nested expression operands
    UNKNOWN
};

// A Value in the expression model. It can be either
// - a field reference which sets the fieldName, or
// - a literal which is a user-supplied value (in the Payload literal) and type (in type).
// If fieldName is non-empty, this is a reference. Otherwise it is a literal value
struct Value {
    String fieldName;
    FieldType type = FieldType::NOT_KNOWN;
    Payload literal;
    K2_PAYLOAD_FIELDS(fieldName, type, literal);
};

// An Expression in the expression model.
// The operation is applied in the order that the children are in the vector. So a binary
// operator like LT would be applied as valueChildren[0] < valueChildren[1]
struct Expression {
    Operation op = Operation::UNKNOWN;
    std::vector<Expression> expressionChildren;
    std::vector<Value> valueChildren;

    bool evaluate(SKVRecord& rec);

    K2_PAYLOAD_FIELDS(op, expressionChildren, valueChildren);

    bool EQ_handler(SKVRecord& rec);
    bool GT_handler(SKVRecord& rec);
    bool GTE_handler(SKVRecord& rec);
    bool LT_handler(SKVRecord& rec);
    bool LTE_handler(SKVRecord& rec);
    bool IS_NULL_handler(SKVRecord& rec);
    bool IS_TYPE_handler(SKVRecord& rec);
    bool STARTS_WITH_handler(SKVRecord& rec);
    bool CONTAINS_handler(SKVRecord& rec);
    bool ENDS_WITH_handler(SKVRecord& rec);
    bool AND_handler(SKVRecord& rec);
    bool OR_handler(SKVRecord& rec);
    bool XOR_handler(SKVRecord& rec);
    bool NOT_handler(SKVRecord& rec);
};

// helper builders which create filter expression trees
template <typename T>
inline Value makeFilterLiteralValue(dto::FieldType fieldType, T&& literal) {
    if (TToFieldType<T>() != fieldType) {
        throw TypeMismatchException();
    }
    Value result;
    result.type = fieldType;
    result.literal.write(literal);
    return result;
}

inline Value makeFilterFieldReferenceValue(const String& fieldName) {
    Value result;
    result.fieldName = fieldName;
    return result;
}

inline Expression makeFilterExpression(Operation op, std::vector<Value>&& valueChildren, std::vector<Expression>&& expressionChildren) {
    return Expression{
        .op = op,
        .expressionChildren = std::move(expressionChildren),
        .valueChildren = std::move(valueChildren),
    };
}

} // ns filter
} // ns dto
} // ns k2
