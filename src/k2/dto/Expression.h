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

#pragma once

#include <functional>
#include <vector>

#include <k2/common/Common.h>
#include <k2/common/VecUtil.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>
#include "FieldTypes.h"
#include "SKVRecord.h"

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
inline std::ostream& operator<<(std::ostream& os, const Operation& op) {
    switch(op) {
        case Operation::EQ: return os << "EQ";
        case Operation::GT: return os << "GT";
        case Operation::GTE: return os << "GTE";
        case Operation::LT: return os << "LT";
        case Operation::LTE: return os << "LTE";
        case Operation::IS_NULL: return os << "IS_NULL";
        case Operation::IS_EXACT_TYPE: return os << "IS_EXACT_TYPE";
        case Operation::STARTS_WITH: return os << "STARTS_WITH";
        case Operation::CONTAINS: return os << "CONTAINS";
        case Operation::ENDS_WITH: return os << "ENDS_WITH";
        case Operation::AND: return os << "AND";
        case Operation::OR: return os << "OR";
        case Operation::XOR: return os << "XOR";
        case Operation::NOT: return os << "NOT";
        default: return os << "UNKNOWN";
    }
}
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
    template<typename T>
    static void _valueStrHelper(const Value& r, std::ostream& os) {
        T obj;
        if (!const_cast<Payload*>(&(r.literal))->shareAll().read(obj)) {
            os << TToFieldType<T>() << "_ERROR_READING";
        }
        else {
            os << obj;
        }
    }
    friend std::ostream& operator<<(std::ostream& os, const Value& r) {
        os << "{fieldName=" << r.fieldName << ", type=" << r.type << ", literal=";
        if (r.fieldName.empty()) {
            return os << "REFERENCE";
        }
        K2_DTO_CAST_APPLY_FIELD_VALUE(_valueStrHelper, r, os);
        return os;
    }

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
    friend std::ostream& operator<<(std::ostream& os, const Expression& r) {
        os << "{op=" << r.op << ", valueChildren=[";
        for (auto& vc: r.valueChildren) {
            os << vc << ",";
        }
        os << "], expressionChildren=[";
        for (auto& ec: r.expressionChildren) {
            os << ec << ", ";
        }
        return os << "]}";
    }
};

// helper builder: creates a value literal
template <typename T>
inline Value makeValueLiteral(T&& literal) {
    Value result{};
    result.type = TToFieldType<T>();
    result.literal.write(literal);
    return result;
}

// helper builder: creates a value reference
inline Value makeValueReference(const String& fieldName) {
    Value result{};
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
