#include <iostream>

#undef K2_PLATFORM_COMPILE

#include "MySKVRecord.h"

int main() {
    k2::dto::Schema my_schema {
        .name = "my_schema",
        .version = 1,
        .fields = std::vector<k2::dto::SchemaField> {
                {k2::dto::FieldType::INT32T, "ID", false, false},
                {k2::dto::FieldType::INT32T, "Value", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    k2::Status valid = my_schema.basicValidation();
    std::cout << "Schema valid: " << valid.getDescription() << std::endl;

    k2::dto::SKVRecord record("collection", std::make_shared<k2::dto::Schema>(my_schema));

    record.serializeNext<int32_t>(5);
    record.serializeNext<int32_t>(10);

    std::optional<int32_t> val = record.deserializeField<int32_t>(0);
    std::cout << "Got from record: " << *val << std::endl;
}
