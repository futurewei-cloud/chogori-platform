#include <k2/transport/Payload.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <vector>
#include <functional>
#include "FieldTypes.h"
#include "SKVRecord.h"
#include <k2/common/VecUtil.h>

namespace k2 {
namespace dto {

// Thrown if the expression is found to be semantically invalid
struct InvalidExpressionException : public std::exception {
    virtual const char* what() const noexcept override{ return "InvalidExpression";}
};

namespace expression {


// The supported operations
enum class Operation : uint8_t {
    EQ,             // A == B. Both A and B must be values
    GT,             // A > B. Both A and B must be values
    GTE,            // A >= B. Both A and B must be values
    LT,             // A < B. Both A and B must be values
    LTE,            // A <= B. Both A and B must be values
    IS_NULL,        // IS_NULL A. A must be a reference value
    IS_EXACT_TYPE,  // A IS_EXACT_TYPE B. A must be a reference, B must be a FieldType literal
    STARTS_WITH,    // A STARTS_WITH B. A must be a string type value, B must be a string literal
    CONTAINS,       // A CONTAINS B. A must be a string type value, B must be a string literal
    ENDS_WITH,      // A ENDS_WITH B. A must be a string type value, B must be a string literal
    AND,            // A AND B. Each of A and B must be a boolean value, or an expression
    OR,             // A OR B. Each of A and B must be a boolean value, or an expression
    XOR,            // A XOR B. Each of A and B must be a boolean value, or an expression
    NOT,            // NOT A. A must be a boolean value, or an expression
    UNKNOWN
};

// A Value in the expression model. It can be either
// - a field reference which sets the fieldName, or
// - a literal which is a user-supplied value (in the Payload literal) and type (in type).
// If fieldName is non-empty, this is a reference. Otherwise it is a literal value
struct Value {
    String fieldName;
    FieldType type = FieldType::NOT_KNOWN;
    Payload literal{Payload::DefaultAllocator};
    K2_PAYLOAD_FIELDS(fieldName, type, literal);
    bool isReference() const { return !fieldName.empty();}
};

// An Expression in the expression model.
// The operation is applied in the order that the children are in the vector. E.g. a binary
// operator like LT would be applied as valueChildren[0] < valueChildren[1]
struct Expression {
    Operation op = Operation::UNKNOWN;
    std::vector<Value> valueChildren;
    std::vector<Expression> expressionChildren;

    // This method is used to evaluate a given record against this filter. Returns true if the record passes
    // evaluation, and false if it does not.
    // The method throws exceptions in cases when we cannot evaluate the record, most notably:
    // TypeMismatchException if there is an expression over incompatible data types
    // InvalidExpressionException if semantically the expression cannot be evaluated (e.g. OR with 1 argument)
    // DeserializationError if we are not able to deserialize a value correctly
    // NoFieldFoundException if we cannot find a field of a given name in the schema
    bool evaluate(SKVRecord& rec);

    K2_PAYLOAD_FIELDS(op, valueChildren, expressionChildren);

    // helper methods used to evaluate particular operation
    bool EQ_handler(SKVRecord& rec);
    bool GT_handler(SKVRecord& rec);
    bool GTE_handler(SKVRecord& rec);
    bool LT_handler(SKVRecord& rec);
    bool LTE_handler(SKVRecord& rec);
    bool IS_NULL_handler(SKVRecord& rec);
    bool IS_EXACT_TYPE_handler(SKVRecord& rec);
    bool STARTS_WITH_handler(SKVRecord& rec);
    bool CONTAINS_handler(SKVRecord& rec);
    bool ENDS_WITH_handler(SKVRecord& rec);
    bool AND_handler(SKVRecord& rec);
    bool OR_handler(SKVRecord& rec);
    bool XOR_handler(SKVRecord& rec);
    bool NOT_handler(SKVRecord& rec);
};

// helper builder: creates a value literal
template <typename T>
inline Value makeValueLiteral(T&& literal) {
    Value result;
    result.type = TToFieldType<T>();
    result.literal.write(literal);
    return result;
}

// helper builder: creates a value reference
inline Value makeValueReference(const String& fieldName) {
    Value result;
    result.fieldName = fieldName;
    return result;
}

// helper builder: creates an expression
inline Expression makeExpression(Operation op, std::vector<Value>&& valueChildren, std::vector<Expression>&& expressionChildren) {
    return Expression{
        .op = op,
        .valueChildren = std::move(valueChildren),
        .expressionChildren = std::move(expressionChildren)
    };
}

} // ns filter
} // ns dto
} // ns k2
