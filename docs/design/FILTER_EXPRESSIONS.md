
Filter expressions are boolean expressions applied on records at runtime. Users can specify these filters as part of their queries, and the server will evaluate each candidate record, based on the rules described below.

# BNF
```
Filter := Expression
Expression := Operator, Operand+
Operator := EQ | GT | GTE | LT | LTE | IS_NULL | IS_EXACT_TYPE | STARTS_WITH | CONTAINS | ENDS_WITH | AND | OR | XOR | NOT
Operand := Value | Expression
Value := Reference | Literal
Literal := FieldType, CPPBuiltInValueOfType
FieldType := "INT16T" | "INT32T" | "INT64_T" | "FLOAT" | "DOUBLE" | "BOOL" | "STRING" | "DECIMALD25" | "DECIMALD50" | "DECIMALD100" | "FIELD_TYPE" | "NULL_T"
CPPBuiltInValueOfType := int16_t | int32_t | int64_t | float | double | bool | string | boost::multiprecision::number<cpp_dec_float<25>> | boost::multiprecision::cpp_dec_float_50 | boost::multiprecision::cpp_dec_float_100 | k2::dto::FieldType | null
Reference := FieldName
FieldName := string
```
* All fields are typed and can be `NULL`.
* `NULL` values are typed i.e. Literal("STRING", null) != Literal("INT16_T", null)
* References are not typed. They refer to a field by name and will take on the type of the found field for any particular record.
* Type conversion is applied during filtering following C++ rules, notably, if type conversion was attempted but no comparable promotion was available, the entire expression fails
* Type conversion is not applied during SKV field deserialization. E.g. if there is a field `{"age": int32}` in the schema, an attempt to deserialize field `{"age": int64}` will fail (throw)
* To aid with schema migration, the IS_TYPE operator provides the capability to select records with exact type.

The processing model is such that the user-provided filter expression is applied on a sequence of candidate records. The records are limited based on other aspects of the query request (e.g. `scanRange[startKey: endKey]`).

# Type mismatch handling
There are cases in which there are type mismatches as we evaluate the filter expression. These can occur when
- the user filters for a similar or wrong type for a literal value, e.g. schema{"age": int32} with expression `Ref{"age"} > Lit{20, int64}`, or `Ref{"age"} > Lit{"20", string}`.
- the user filters based on comparison of differently typed field references, e.g. schema{"age": int32, "order_count": int64} with expression `Ref{"age"} < Ref{"order_count"}`

The processing logic biases towards strict logic when faced with type mismatches. In general, the processing is as close as possible to C++'s handling of types. That is, if the types are similar and can be implicitly promoted to perform the desired predicate (e.g. `int32 < int64`), then we do so. Otherwise, the candidate record is rejected.

# Conflicting schema version changes
When the user alters their schema and change the type of a field, we do not backfill or cast the values automatically. We expect the user to either be able to handle records of multiple versions, or perform their own backfill (via push-down for better performance).

## Compatible schema changes
if a field is changed in a compatible way, e.g. `{"age": int32} -> {"age": int64}`, then there would not be any change to how we process queries. However, when the user deserializes the field `"age"`, they would have to be explicit as to the type they are deserializing as - we will throw exception if the user attempts to deserialize the int32 field as int64. The necessary type information is provided for every SKVRecord to the user via the embedded schema in the SKVRecord.

## Non-compatible schema changes
These are changes which are not type-compatible in a C++ sense. E.g. `{"age": int32} -> {"age": "string"}`. In general, it is a better design to avoid such changes to the schema. Instead it is better to create a separate column for the string version of "age". However K2 does not prevent such changes as they are not destructive. If the user performs such a schema alteration, they will be able to write records with different versions and so the user would have to be ready to handle results of different schema versions.

The query processing adheres to c++ rules and records which cause type mismatch during comparison are considered non-matching. For example, given two records with re-typed column "age":
- R1: `schema{"age": int32}`, `{"age", 20}`
- R2: `schema{"age": string}`, `{"age", "20"}`

An attempt to filter with predicate `Ref{"age"} == Lit{20, int64}` will only return R1. Similarly, filter with `Ref{"age"} == Lit{"20", string}` would only return R2. We provide the `IS_TYPE` operator so that the users can discover records with fields of a particular type. This is useful for backfills on schema migration.

# NULL handling
NULLs are typed and treated as such in all above cases. That is a mismatch of type in any predicate of the expression causes the record to be invalidated. The same applies for expressions when we consider the NULL_FIRST or NULL_LAST ordering of values in a column. For example, given records with schema for "age" -> NULL_FIRST and values:
- R1: `schema{"age": int32/NULL_FIRST}`, `{"age", NULL}`
- R2: `schema{"age": string/NULL_FIRST}`, `{"age", NULL}`
- R3: `schema{"age": int32/NULL_FIRST}`, `{"age", 20}`
- R4: `schema{"age": string/NULL_FIRST}`, `{"age", "20"}`

the predicate `Ref{"age"} < Lit{30, int32}` will return records R1, R3 only since they are the ones that match a compatible type (int32). Similarly, the predicate `Ref{"age"} == Lit{NULL, string}` will return the record R2 since this is the only record that matches `NULL` value of `string` type.

## NULL order mismatch
in cases where there is an ordering mismatch on columns, e.g. `schema1={"age": int32/NULL_FIRST, "order_count": int64/NULL_LAST}, ` with filter `Ref{"age"} < Ref{"order_count"}` we will fall back to perform NULL_FIRST ordering, e.g.
- this record passes the filter: `[{"age", NULL}, {"order_count", 20}]`
- this record doesn't pass the filter: `[{"age", 20}, {"order_count", NULL}]`

# Potential downsides
- short-circuit isn't available. We have to exhaust all predicates in the expression to detect if there are any type-mismatched comparisons.
