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
#include <k2/dto/shared/FieldTypes.h>
#include <k2/dto/shared/SKVRecord.h>
#include "Log.h"

namespace k2 {
namespace dto {

// Thrown if the expression is found to be semantically invalid
struct InvalidExpressionException : public std::exception {
    InvalidExpressionException(String msg): _msg(std::move(msg)){}
    virtual const char* what() const noexcept override{ return _msg.c_str();}
private:
    String _msg;
};

namespace expression {


// The supported operations
K2_DEF_ENUM(Operation,
    EQ,             /* A == B. Both A and B must be values */
    GT,             /* A > B. Both A and B must be values */
    GTE,            /* A >= B. Both A and B must be values */
    LT,             /* A < B. Both A and B must be values */
    LTE,            /* A <= B. Both A and B must be values */
    IS_NULL,        /* IS_NULL A. A must be a reference value */
    IS_EXACT_TYPE,  /* A IS_EXACT_TYPE B. A must be a reference, B must be a FieldType literal */
    STARTS_WITH,    /* A STARTS_WITH B. A must be a string type value, B must be a string literal */
    CONTAINS,       /* A CONTAINS B. A must be a string type value, B must be a string literal */
    ENDS_WITH,      /* A ENDS_WITH B. A must be a string type value, B must be a string literal */
    AND,            /* A AND B. Each of A and B must be a boolean value, or an expression */
    OR,             /* A OR B. Each of A and B must be a boolean value, or an expression */
    XOR,            /* A XOR B. Each of A and B must be a boolean value, or an expression */
    NOT,            /* NOT A. A must be a boolean value, or an expression */
    UNKNOWN
);

// A Value in the expression model. It can be either
// - a field reference which sets the fieldName, or
// - a literal which is a user-supplied value (in the Payload literal) and type (in type).
// If fieldName is non-empty, this is a reference. Otherwise it is a literal value
struct Value {
    String fieldName;
    FieldType type = FieldType::NOT_KNOWN;
    Payload literal{Payload::DefaultAllocator()};
    bool isReference() const { return !fieldName.empty();}

    K2_PAYLOAD_FIELDS(fieldName, type, literal);

    template<typename T, typename Stream>
    static void _valueStrHelper(const Value& r, Stream& os) {
        T obj;
        if (!const_cast<Payload*>(&(r.literal))->shareAll().read(obj)) {
            os << TToFieldType<T>() << "_ERROR_READING";
        }
        else {
            os << obj;
        }
    }

    // custom json conversion
    friend void to_json(nlohmann::json& j, const Value& o) {
        std::ostringstream otype;
        otype << o.type;

        std::ostringstream lit;
        if (o.isReference()) {
            lit << "REFERENCE";
        }
        else {
            if (o.type != FieldType::NULL_T && o.type != FieldType::NOT_KNOWN && o.type != FieldType::NULL_LAST) {
                // no need to log the other types - we wrote what they are above.
                try {
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_valueStrHelper, o, lit);
                }
                catch (const std::exception& e) {
                    // just in case, log the exception here so that we can do something about it
                    K2LOG_E(log::dto, "Caught exception in expression serialize: {}", e.what());
                    lit << "!!!EXCEPTION!!!: " << e.what();
                }
            }
        }
        j = {
            {"fieldName", o.fieldName},
            {"type", k2::HexCodec::encode(otype.str())},
            {"literal", k2::HexCodec::encode(lit.str())}
        };
    }
    friend void from_json(const nlohmann::json&, Value&) {
        throw std::runtime_error("Value type does not support construct from json");
    }

    friend std::ostream& operator<<(std::ostream&os, const Value& v) {
        nlohmann::json j = v;
        return os << j.dump();
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

    // Recursively copies the payloads if the expression's values and children. This is used so that the
    // memory of the payloads will be allocated in the context of the current thread.
    void copyPayloads();

    K2_PAYLOAD_FIELDS(op, valueChildren, expressionChildren);
    K2_DEF_FMT(Expression, op, valueChildren, expressionChildren);

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
    if(isNan<T>(literal)){
            throw NaNError("NaN type in serialization");
    }

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
