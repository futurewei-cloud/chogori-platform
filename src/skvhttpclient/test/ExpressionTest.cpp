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

#define CATCH_CONFIG_MAIN

#include<vector>
#include <skv/dto/Expression.h>
#include <skv/dto/SKVRecord.h>
#include "catch.hpp"

using namespace skv::http;

namespace k2d = skv::http::dto;
namespace k2e = skv::http::dto::expression;

namespace skv::http::log {
inline thread_local k2::logging::Logger k23si("k2::k23si_test");
}

using K2Exp = skv::http::dto::expression::Expression;
using K2Val = skv::http::dto::expression::Value;

struct TestCase {
    String name;
    K2Exp expr;
    k2d::SKVRecord rec;
    std::optional<bool> expectedResult;
    std::exception_ptr expectedException;
    bool run() {
        return expr.evaluate(rec);
    }
};

k2d::SKVRecord makeRec() {
    auto schema = std::make_shared<k2d::Schema>();
    schema->name = "test_schema";
    schema->version = 1;
    schema->fields = std::vector<k2d::SchemaField>{
        {k2d::FieldType::STRING, "str", false, false},
        {k2d::FieldType::STRING, "strNL", false, true},
        {k2d::FieldType::STRING, "strEMPTY", false, true},
        {k2d::FieldType::STRING, "strNS", false, true},

        {k2d::FieldType::INT16T, "int16S", false, false},
        {k2d::FieldType::INT16T, "int16M", false, false},
        {k2d::FieldType::INT16T, "int16L", false, false},
        {k2d::FieldType::INT16T, "int16NS", false, false},

        {k2d::FieldType::INT32T, "int32S", false, false},
        {k2d::FieldType::INT32T, "int32M", false, false},
        {k2d::FieldType::INT32T, "int32L", false, false},
        {k2d::FieldType::INT32T, "int32NS", false, false},

        {k2d::FieldType::INT64T, "int64S", false, false},
        {k2d::FieldType::INT64T, "int64M", false, false},
        {k2d::FieldType::INT64T, "int64L", false, false},
        {k2d::FieldType::INT64T, "int64NS", false, false},

        {k2d::FieldType::FLOAT, "floatS", false, false},
        {k2d::FieldType::FLOAT, "floatM", false, false},
        {k2d::FieldType::FLOAT, "floatL", false, false},
        {k2d::FieldType::FLOAT, "floatNS", false, false},

        {k2d::FieldType::DOUBLE, "doubleS", false, false},
        {k2d::FieldType::DOUBLE, "doubleM", false, false},
        {k2d::FieldType::DOUBLE, "doubleL", false, false},
        {k2d::FieldType::DOUBLE, "doubleNS", false, false},

        {k2d::FieldType::BOOL, "boolF", false, false},
        {k2d::FieldType::BOOL, "boolT", false, false},
        {k2d::FieldType::BOOL, "boolNS", false, false},
    };

    schema->setPartitionKeyFieldsByName(std::vector<String>{"str"});
    schema->setRangeKeyFieldsByName(std::vector<String>{"strNL"});

    k2d::SKVRecordBuilder builder("collection", schema);

    builder.serializeNext<String>("Baggins");
    builder.serializeNext<String>("Bilbo");
    builder.serializeNext<String>("");
    builder.serializeNull();

    builder.serializeNext<int16_t>(std::numeric_limits<int16_t>::min());
    builder.serializeNext<int16_t>(-5);
    builder.serializeNext<int16_t>(std::numeric_limits<int16_t>::max());
    builder.serializeNull();

    builder.serializeNext<int32_t>(std::numeric_limits<int32_t>::min());
    builder.serializeNext<int32_t>(0);
    builder.serializeNext<int32_t>(std::numeric_limits<int32_t>::max());
    builder.serializeNull();

    builder.serializeNext<int64_t>(std::numeric_limits<int64_t>::min());
    builder.serializeNext<int64_t>(5);
    builder.serializeNext<int64_t>(std::numeric_limits<int64_t>::max());
    builder.serializeNull();

    builder.serializeNext<float>(std::numeric_limits<float>::min());
    builder.serializeNext<float>(10.123);
    builder.serializeNext<float>(std::numeric_limits<float>::max());
    builder.serializeNull();

    builder.serializeNext<double>(std::numeric_limits<double>::min());
    builder.serializeNext<double>(20.312);
    builder.serializeNext<double>(std::numeric_limits<double>::max());
    builder.serializeNull();

    builder.serializeNext<bool>(false);
    builder.serializeNext<bool>(true);
    builder.serializeNull();
    return builder.build();
}

void runner(std::vector<TestCase>& tcases) {
    for (auto& tcase: tcases) {
        K2LOG_I(log::k23si, "tcase name: {}", tcase.name);
        try {
            bool result = tcase.run();
            if (tcase.expectedResult.has_value()) {
                REQUIRE(tcase.expectedResult.value() == result);
            }
            else {
                REQUIRE(false);
            }
        }
        catch(k2d::NoFieldFoundException&) {
            REQUIRE(tcase.expectedException);
            try{ std::rethrow_exception(tcase.expectedException); }
            catch(k2d::NoFieldFoundException&) {}
            catch(...){ REQUIRE(false); }
        }
        catch(k2d::TypeMismatchException&) {
            REQUIRE(tcase.expectedException);
            try{ std::rethrow_exception(tcase.expectedException); }
            catch(k2d::TypeMismatchException&) {}
            catch(...){ REQUIRE(false); }
        }
        catch (k2d::DeserializationError&) {
            REQUIRE(tcase.expectedException);
            try{ std::rethrow_exception(tcase.expectedException); }
            catch(k2d::DeserializationError&) {}
            catch(...){ REQUIRE(false); }
        }
        catch (k2d::InvalidExpressionException&) {
            REQUIRE(tcase.expectedException);
            try{ std::rethrow_exception(tcase.expectedException); }
            catch(k2d::InvalidExpressionException&) {}
            catch(...){ REQUIRE(false); }
        }
        catch (...) {
            REQUIRE(false);
        }
    }
}

TEST_CASE("NaN expressions"){
    k2::logging::Logger::threadLocalLogLevel = k2::logging::LogLevel::VERBOSE;

    try{
        k2e::makeValueLiteral<double>(nan("1"));
        REQUIRE(false);
    }catch(k2d::NaNError &){
        std::cout << "Expression with NaN literal cannot be made." << std::endl;
    }

    try{
        Decimal64 y(nan("1"));
        k2e::makeValueLiteral<Decimal64>(std::move(y));
        REQUIRE(false);
    }catch(k2d::NaNError &){
        std::cout << "Expression with NaN decimal64 literal cannot be made." << std::endl;
    }

    try{
        Decimal128 y(nan("1"));
        k2e::makeValueLiteral<Decimal128>(std::move(y));
        REQUIRE(false);
    }catch(k2d::NaNError &){
        std::cout << "Expression with NaN decimal128 literal cannot be made." << std::endl;
    }
}

TEST_CASE("Float expressions"){
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "gt: two floats gt",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<float>(2.0), k2e::makeValueLiteral<float>(1.1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: two floats not gt",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<float>(0.5), k2e::makeValueLiteral<float>(1.1)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});

    Decimal64 v1(101.5001);
    Decimal64 v2(101.5002);
    cases.push_back(TestCase{
        .name = "gt: two decimals not gt",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<Decimal64>(std::move(v1)), k2e::makeValueLiteral<Decimal64>(std::move(v2))), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});

    Decimal128 x1(101.5002);
    Decimal128 x2(101.5001);
    cases.push_back(TestCase{
        .name = "gt: two decimals gt",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<Decimal128>(std::move(x1)), k2e::makeValueLiteral<Decimal128>(std::move(x2))), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});

    runner(cases);
}

TEST_CASE("Empty expressions") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name="empty expression",
        .expr={},
        .rec={},
        .expectedResult={true},
        .expectedException={}});
    cases.push_back(TestCase{
        .name="empty expression with record",
        .expr={},
        .rec=makeRec(),
        .expectedResult={true},
        .expectedException={}});
    runner(cases);
}

TEST_CASE("Invalid expressions") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "CONTAINS with no args",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "ENDS_WITH with no args",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "STARTS_WITH with no args",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "EQ op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "GT op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::GT, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "GTE op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "LT op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::LT, {}, {})},
        .rec = {},
         .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "LTE op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "NOT op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, {}, {})},
        .rec = {},
         .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "AND op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "OR op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "IS_EXACT_TYPE op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "IS_NULL op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::IS_NULL, {}, {})},
        .rec = {},
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "UNKNOWN op with no args",
        .expr = {k2e::makeExpression(k2e::Operation::UNKNOWN, {}, {})},
        .rec = {},
        .expectedResult = {true},
        .expectedException = {}});


    cases.push_back(TestCase{
        .name = "CONTAINS with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "ENDS_WITH with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "STARTS_WITH with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "EQ op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "GT op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::GT, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "GTE op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "LT op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::LT, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "LTE op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "NOT op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "AND op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "OR op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "IS_EXACT_TYPE op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "IS_NULL op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::IS_NULL, {}, {})},
        .rec = makeRec(),
        .expectedResult = std::nullopt,
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "UNKNOWN op with no args and record",
        .expr = {k2e::makeExpression(k2e::Operation::UNKNOWN, {}, {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});

    runner(cases);
}

TEST_CASE("Test CONTAINS") {
    std::vector<TestCase> cases;
    // invalid cases
    cases.push_back(TestCase{
        .name = "contains with expression",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, {}, make_vec<K2Exp>(K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "contains with 2 expressions",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, {}, make_vec<K2Exp>(K2Exp{}, K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "contains with expression and value",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(K2Val{}), make_vec<K2Exp>(K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    // 2 values but types aren't as expected
    cases.push_back(TestCase{
        .name = "contains: 2 values wrong type",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(K2Val{}, K2Val{}),{})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "contains: str ref, wrong_type",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("str"), K2Val{}), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "contains: 2 str refs",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Reference in a literal",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueLiteral(String("Bagginses")), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Reference not in a literal",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueLiteral(String("agginses")), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "contains: Literal in a Literal",
        .expr = {k2e::makeExpression(
                    k2e::Operation::CONTAINS,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral(String("agginses")),
                        k2e::makeValueLiteral(String("agg"))
                    ),
                    {}
            )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Literal not in a Literal",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueLiteral(String("agginses")), k2e::makeValueLiteral(String("igg"))), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Literal not in a Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueLiteral(String("agginses"))), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Literal in a Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueLiteral(String("agg"))), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Literal in a non-existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("strNE"), k2e::makeValueLiteral(String("agg"))), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::NoFieldFoundException())}});
    cases.push_back(TestCase{
        .name = "contains: Reference in a non-existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("strNE"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::NoFieldFoundException())}});
    cases.push_back(TestCase{
        .name = "contains: Literal in a non-set existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("strNS"), k2e::makeValueLiteral<String>("agg")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Reference in a non-set existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("strNS"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Literal in an empty existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("strEMPTY"), k2e::makeValueLiteral<String>("agg")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "contains: Reference in an empty existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::CONTAINS, make_vec<K2Val>(k2e::makeValueReference("strEMPTY"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test STARTS_WITH") {
    std::vector<TestCase> cases;
    // invalid cases
    cases.push_back(TestCase{
        .name = "starts_with: with expression",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, {}, make_vec<K2Exp>(K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "starts_with: with 2 expressions",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, {}, make_vec<K2Exp>(K2Exp{}, K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "starts_with: with expression and value",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(K2Val{}), make_vec<K2Exp>(K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    // 2 values but types aren't as expected
    cases.push_back(TestCase{
        .name = "starts_with: 2 values wrong type",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(K2Val{}, K2Val{}), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "starts_with: str ref, wrong_type",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), K2Val{}), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "starts_with: 2 str refs",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Reference in a literal",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueLiteral(String("Bagginses")), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Reference not in a literal",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueLiteral(String("agginses")), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "starts_with: Literal in a Literal",
        .expr = {k2e::makeExpression(
            k2e::Operation::STARTS_WITH,
            make_vec<K2Val>(
                k2e::makeValueLiteral(String("agginses")),
                k2e::makeValueLiteral(String("agg"))),
            {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Literal not in a Literal",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueLiteral(String("agginses")), k2e::makeValueLiteral(String("igg"))), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Literal not in a Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueLiteral(String("agginses"))), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Literal in a Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueLiteral(String("Bagg"))), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Literal in a non-existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNE"), k2e::makeValueLiteral(String("Bagg"))), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::NoFieldFoundException())}});
    cases.push_back(TestCase{
        .name = "starts_with: Reference in a non-existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNE"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::NoFieldFoundException())}});
    cases.push_back(TestCase{
        .name = "starts_with: Literal in a non-set existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNS"), k2e::makeValueLiteral<String>("Bagg")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Reference in a non-set existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNS"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Literal in an empty existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("strEMPTY"), k2e::makeValueLiteral<String>("Bagg")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "starts_with: Reference in an empty existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::STARTS_WITH, make_vec<K2Val>(k2e::makeValueReference("strEMPTY"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test ENDS_WITH") {
    std::vector<TestCase> cases;
    // invalid cases
    cases.push_back(TestCase{
        .name = "ends_with: with expression",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, {}, make_vec<K2Exp>(K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "ends_with: with 2 expressions",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, {}, make_vec<K2Exp>(K2Exp{}, K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "ends_with: with expression and value",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(K2Val{}), make_vec<K2Exp>(K2Exp{}))},
        .rec = {},
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    // 2 values but types aren't as expected
    cases.push_back(TestCase{
        .name = "ends_with: 2 values wrong type",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(K2Val{}, K2Val{}), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "ends_with: str ref, wrong_type",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), K2Val{}), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "ends_with: 2 str refs",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Reference in a literal",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueLiteral(String("AbadaBaggins")), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Reference not in a literal",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueLiteral(String("agginses")), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "ends_with: Literal in a Literal",
        .expr = {k2e::makeExpression(
            k2e::Operation::ENDS_WITH,
            make_vec<K2Val>(
                k2e::makeValueLiteral(String("agginses")),
                k2e::makeValueLiteral(String("inses"))),
            {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Literal not in a Literal",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueLiteral(String("agginses")), k2e::makeValueLiteral(String("igg"))), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Literal not in a Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueLiteral(String("agginses"))), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Literal in a Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("str"), k2e::makeValueLiteral(String("ggins"))), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Literal in a non-existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNE"), k2e::makeValueLiteral(String("ggins"))), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::NoFieldFoundException())}});
    cases.push_back(TestCase{
        .name = "ends_with: Reference in a non-existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNE"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::NoFieldFoundException())}});
    cases.push_back(TestCase{
        .name = "ends_with: Literal in a non-set existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNS"), k2e::makeValueLiteral<String>("ggins")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Reference in a non-set existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("strNS"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Literal in an empty existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("strEMPTY"), k2e::makeValueLiteral<String>("Bagg")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "ends_with: Reference in an empty existing Reference",
        .expr = {k2e::makeExpression(k2e::Operation::ENDS_WITH, make_vec<K2Val>(k2e::makeValueReference("strEMPTY"), k2e::makeValueReference("str")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test EQ") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "equals: two literals equal",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int32_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "equals: two literals not equal",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int32_t>(2)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "equals: two literals equal compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int64_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "equals: two literals not equal compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int64_t>(2)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "equals: reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueReference("int16M"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "equals: two references",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueReference("int32M"), k2e::makeValueReference("int32M")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "equals: two non-set references",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueReference("int64NS")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "equals: non-set reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::EQ, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test GT") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "gt: two literals not gt",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int32_t>(2)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: two literals gt",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(2), k2e::makeValueLiteral<int32_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: two literals gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(5), k2e::makeValueLiteral<int64_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: two literals not gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(-20), k2e::makeValueLiteral<int64_t>(-10)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueReference("int16L"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: two references",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueReference("int32M"), k2e::makeValueReference("int16M")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: two non-set references",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueReference("int64NS")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gt: non-set reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::GT, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test GTE") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "gte: two literals not gt",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int32_t>(2)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: two literals gt",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(2), k2e::makeValueLiteral<int32_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: two literals gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(5), k2e::makeValueLiteral<int64_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: two literals eq compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int64_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: two literals not gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(-20), k2e::makeValueLiteral<int64_t>(-10)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueReference("int16L"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: two references",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueReference("int32M"), k2e::makeValueReference("int16M")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: two non-set references",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueReference("int64NS")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: non-set reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "gte: non-set reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::GTE, make_vec<K2Val>(k2e::makeValueLiteral<int64_t>(-5),k2e::makeValueReference("int32NS")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test LT") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "lt: two literals not gt",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int32_t>(2)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lt: two literals gt",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(2), k2e::makeValueLiteral<int32_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lt: two literals gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(5), k2e::makeValueLiteral<int64_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lt: two literals not gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(-20), k2e::makeValueLiteral<int64_t>(-10)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lt: reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueReference("int16L"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lt: two references",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueReference("int32M"), k2e::makeValueReference("int16M")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lt: two non-set references",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueReference("int64NS")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lt: non-set reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::LT, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test LTE") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "lte: two literals not gt",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int32_t>(2)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: two literals gt",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(2), k2e::makeValueLiteral<int32_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: two literals gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(5), k2e::makeValueLiteral<int64_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: two literals eq compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1), k2e::makeValueLiteral<int64_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: two literals not gt compatible types",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(-20), k2e::makeValueLiteral<int64_t>(-10)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueReference("int16L"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: two references",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueReference("int32M"), k2e::makeValueReference("int16M")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: two non-set references",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueReference("int64NS")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: non-set reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueReference("int32NS"), k2e::makeValueLiteral<int64_t>(-5)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "lte: non-set reference and literal",
        .expr = {k2e::makeExpression(k2e::Operation::LTE, make_vec<K2Val>(k2e::makeValueLiteral<int64_t>(-5), k2e::makeValueReference("int32NS")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test NOT") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "not: non-bool literal",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(1)), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "not: bool literal true",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, make_vec<K2Val>(k2e::makeValueLiteral<bool>(true)), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "not: bool literal false",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, make_vec<K2Val>(k2e::makeValueLiteral<bool>(false)), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "not: non-bool reference",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, make_vec<K2Val>(k2e::makeValueReference("int16S")), {})},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "not: bool reference true",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, make_vec<K2Val>(k2e::makeValueReference("boolT")), {})},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "not: bool reference false",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, make_vec<K2Val>(k2e::makeValueReference("boolF")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "not: bool non-set reference",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, make_vec<K2Val>(k2e::makeValueReference("boolNS")), {})},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "not: bool bad type sub-expression",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<String>("bad_value"),
                    k2e::makeValueLiteral<bool>(true)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});

    cases.push_back(TestCase{
        .name = "not: bool true sub-expression",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(10),
                    k2e::makeValueLiteral<int16_t>(5)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "not: bool false sub-expression",
        .expr = {k2e::makeExpression(k2e::Operation::NOT, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(5),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test AND") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "and: bool A expr false, B expr false",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(30),
                    k2e::makeValueLiteral<int16_t>(50)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(10),
                    k2e::makeValueLiteral<int16_t>(20)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A expr true, B expr false",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(10),
                    k2e::makeValueLiteral<int16_t>(20)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A expr false, B expr true",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(30),
                    k2e::makeValueLiteral<int16_t>(50)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "and: bool A expr true, B expr true",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool 3 expressions",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "and: 1 expr",
        .expr = {k2e::makeExpression(k2e::Operation::AND, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "and: 1 value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(30)),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "and: bool A expr true, B wrong type value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<int16_t>(50)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "and: bool A expr true, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A expr true, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});


    cases.push_back(TestCase{
        .name = "and: bool A expr false, B wrong type value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<int16_t>(50)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "and: bool A expr false, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A expr false, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A false value, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false),
                k2e::makeValueLiteral<bool>(false)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A true value, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true),
                k2e::makeValueLiteral<bool>(false)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A false value, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false),
                k2e::makeValueLiteral<bool>(true)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "and: bool A true value, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::AND,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true),
                k2e::makeValueLiteral<bool>(true)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test OR") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "or: bool A expr false, B expr false",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(30),
                    k2e::makeValueLiteral<int16_t>(50)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(10),
                    k2e::makeValueLiteral<int16_t>(20)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A expr true, B expr false",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(10),
                    k2e::makeValueLiteral<int16_t>(20)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A expr false, B expr true",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(30),
                    k2e::makeValueLiteral<int16_t>(50)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "or: bool A expr true, B expr true",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool 3 expressions",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "or: 1 expr",
        .expr = {k2e::makeExpression(k2e::Operation::OR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "or: 1 value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(30)),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "or: bool A expr true, B wrong type value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<int16_t>(50)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "or: bool A expr true, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A expr true, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});


    cases.push_back(TestCase{
        .name = "or: bool A expr false, B wrong type value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<int16_t>(50)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "or: bool A expr false, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A expr false, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A false value, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false),
                k2e::makeValueLiteral<bool>(false)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A true value, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true),
                k2e::makeValueLiteral<bool>(false)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A false value, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false),
                k2e::makeValueLiteral<bool>(true)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "or: bool A true value, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::OR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true),
                k2e::makeValueLiteral<bool>(true)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test XOR") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "xor: bool A expr false, B expr false",
        .expr = {k2e::makeExpression(k2e::Operation::XOR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(30),
                    k2e::makeValueLiteral<int16_t>(50)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(10),
                    k2e::makeValueLiteral<int16_t>(20)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A expr true, B expr false",
        .expr = {k2e::makeExpression(k2e::Operation::XOR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(10),
                    k2e::makeValueLiteral<int16_t>(20)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A expr false, B expr true",
        .expr = {k2e::makeExpression(k2e::Operation::XOR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(30),
                    k2e::makeValueLiteral<int16_t>(50)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});

    cases.push_back(TestCase{
        .name = "xor: bool A expr true, B expr true",
        .expr = {k2e::makeExpression(k2e::Operation::XOR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool 3 expressions",
        .expr = {k2e::makeExpression(k2e::Operation::XOR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {}),
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(20),
                    k2e::makeValueLiteral<int16_t>(10)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "xor: 1 expr",
        .expr = {k2e::makeExpression(k2e::Operation::XOR, {}, make_vec<K2Exp>(
            k2e::makeExpression(k2e::Operation::GT,
                make_vec<K2Val>(
                    k2e::makeValueLiteral<int16_t>(50),
                    k2e::makeValueLiteral<int16_t>(30)
                ),
                {})
        ))},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "xor: 1 value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(k2e::makeValueLiteral<int32_t>(30)),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "xor: bool A expr true, B wrong type value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<int16_t>(50)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "xor: bool A expr true, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A expr true, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(50),
                        k2e::makeValueLiteral<int16_t>(30)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});


    cases.push_back(TestCase{
        .name = "xor: bool A expr false, B wrong type value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<int16_t>(50)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::TypeMismatchException())}});
    cases.push_back(TestCase{
        .name = "xor: bool A expr false, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A expr false, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false)
            ),
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(30),
                        k2e::makeValueLiteral<int16_t>(50)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A false value, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false),
                k2e::makeValueLiteral<bool>(false)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A true value, B false value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true),
                k2e::makeValueLiteral<bool>(false)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A false value, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(false),
                k2e::makeValueLiteral<bool>(true)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "xor: bool A true value, B true value",
        .expr = {k2e::makeExpression(k2e::Operation::XOR,
            make_vec<K2Val>(
                k2e::makeValueLiteral<bool>(true),
                k2e::makeValueLiteral<bool>(true)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    runner(cases);
}

TEST_CASE("Test IS_NULL") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "is_null: not a reference",
        .expr = {k2e::makeExpression(k2e::Operation::IS_NULL,
            make_vec<K2Val>(
                k2e::makeValueLiteral<int32_t>(20)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "is_null: ref not null",
        .expr = {k2e::makeExpression(k2e::Operation::IS_NULL,
            make_vec<K2Val>(
                k2e::makeValueReference("int16L")
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "is_null: ref is null",
        .expr = {k2e::makeExpression(k2e::Operation::IS_NULL,
            make_vec<K2Val>(
                k2e::makeValueReference("int16NS")
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "is_null: expression",
        .expr = {k2e::makeExpression(k2e::Operation::IS_NULL,
            {},
            make_vec<K2Exp>(
                k2e::makeExpression(k2e::Operation::GT,
                    make_vec<K2Val>(
                        k2e::makeValueLiteral<int16_t>(20),
                        k2e::makeValueLiteral<int16_t>(10)
                    ),
                    {})
            )
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    runner(cases);
}

TEST_CASE("Test IS_EXACT_TYPE") {
    std::vector<TestCase> cases;
    cases.push_back(TestCase{
        .name = "is_exact_type: bool type match",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE,
            make_vec<K2Val>(
                k2e::makeValueReference("boolT"),
                k2e::makeValueLiteral<k2d::FieldType>(k2d::FieldType::BOOL)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "is_exact_type: bool type not match",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE,
            make_vec<K2Val>(
                k2e::makeValueReference("boolT"),
                k2e::makeValueLiteral<k2d::FieldType>(k2d::FieldType::INT16T)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "is_exact_type: int16 type match",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE,
            make_vec<K2Val>(
                k2e::makeValueReference("int16S"),
                k2e::makeValueLiteral<k2d::FieldType>(k2d::FieldType::INT16T)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {true},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "is_exact_type: int16 type not match int32",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE,
            make_vec<K2Val>(
                k2e::makeValueReference("int16S"),
                k2e::makeValueLiteral<k2d::FieldType>(k2d::FieldType::INT32T)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {false},
        .expectedException = {}});
    cases.push_back(TestCase{
        .name = "is_exact_type: first value not a ref",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE,
            make_vec<K2Val>(
                k2e::makeValueLiteral<k2d::FieldType>(k2d::FieldType::INT32T),
                k2e::makeValueLiteral<k2d::FieldType>(k2d::FieldType::INT32T)
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    cases.push_back(TestCase{
        .name = "is_exact_type: second type not a literal",
        .expr = {k2e::makeExpression(k2e::Operation::IS_EXACT_TYPE,
            make_vec<K2Val>(
                k2e::makeValueReference("int16S"),
                k2e::makeValueReference("int32S")
            ),
            {}
        )},
        .rec = makeRec(),
        .expectedResult = {},
        .expectedException = {std::make_exception_ptr(k2d::InvalidExpressionException(""))}});
    runner(cases);
}
