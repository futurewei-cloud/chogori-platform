### Overview

In our SKV storage, the user's schema defines a record's partition and range keys. The schema defines the 
types, order, and NULL first or last properties of the keys. This document applies to both 
partition and range keys equally so I will only refer to "keys" for the rest of the document.


The client library will convert fields in a SKVRecord to keys, where the keys are binary strings. We want 
this for several reasons. First, routing a request based on range partitioning is much easier with strings. 
Second, comparisons on the server are much simpler to implement (and likely faster) with flat strings rather 
than across the individual fields of a record. Lastly, strings give us more flexibility in the indexer by 
letting us use trie-based structures.


Our encoding of user fields to key strings should have these properties (described in more detail 
below):
1. Uniqueness. If fields are different than the resulting encoding must be different.
2. Ordering. The ordering of the keys must be the same as the field values, both on a field by field basis and as an overall key.
3. NULL first or last ordering support.
4. Decode support. Although not strictly necessary, it gives us more design freedom and better debugging support if we can recover individual field values from a key string.
5. No trailing null bytes. This is purely to support the HOT indexer and therefore is just a nice-to-have.


### Uniqueness

Uniqueness is not an issue for the encoding of individual fields except for some edge cases such as 
positive or negative zero in a numeric type. A problem can occur though with a naive approach to constructing 
the compound key. Consider a simple key schema with two string fields: 
{("FirstName", String), ("LastName", String)}


Let's say that we have two records: {"Bob", "urns"} and {"Bo", "burns"}. As each field is a string, we can 
assume we can use the field value directly with no extra encoding for now. But our naive approach to 
construct the compound key is simple concantenation, so we end up with the two key strings: 
"Boburns" and "Boburns". The user considers these unqiue records, but our encoding made them alias 
each other in the rest of the system, which is a problem.


The solution to this is to insert a separator between the encoded fields. Purely from a uniqueness 
perspective, the separator can be an arbitrary value but it does need to be escaped in the encoded 
fields. For example, if we choose 'b' as our separator then in our example we still cannot distinguish 
the records without escaping. For now, let us assume our String type is a true C-String with no NULL bytes, 
then we can choose '\0' as our separator (escaping is discussed in a later section). Continuing the example, 
we end up with "Bob\0urns" and "Bo\0burns", as the encoded strings which are unique.


### Ordering

For ordering, it turns out that we cannot use an arbitrary separator even with escaping. To match user 
expectations and to efficiently process queries, the keys need to be ordered on a field by field basis. 
To clarify, in our example above, all records need to be strictly ordered by FirstName and only when 
FirstNames are equal does LastName affect ordering.


Let's try to use 'c' as our separator to show that an arbitrary separator does not work. Once again we will 
use the example from above and save the escaping method for later. Our encoded keys will be "Bobcurns" and 
"Bocburns". They are unique but the order is broken. In the orginal fields, "Bob" > "Bo" because "Bo" is a 
prefix, but in our encoding "Bobcurns" < "Bocburns" because 'b' < 'c'.


Therefore the first byte of our separator needs to compare less than all other bytes to preserve the 
ordering of fields that may be prefixes of each other. So we can use the NULL byte. Furthermore, 
we are going to call this the field terminator (i.e. even the last field has the terminator at the end) and 
we will use the CockroachDB approach of a two-byte sequence for the terminator, namely {0x00, 0x01}. This 
also prevents trailing NULL bytes which lets us support HOT.


### Escaping

If we do not support byte-sequence key field types, then we do not need to escape NULL bytes. If we 
do, we can support it as a different type in the schema (e.g. "Bytes") to avoid the escaping cost in the 
case where the user only wants to use a String type with no NULL characters.


The strategy is to escape 0x00 with the two-byte sequence {0x00, 0xFF}. Now we can show why the terminator 
needs to be {0x00, 0x01}. Consider a key schema with two Bytes fields, {("Region", Bytes), ("Account", Bytes)}, and two records with that schema: (0x03, 0xFF 0x01) and (0x03 0x00, 0x02). The first record is less than the 
second because the "Region" field in the first record is a prefix of the second record's field.


Now we escape, and demonstrate what happens with using only 0x00 as a separator. The encoded keys become: 
(0x03 0x00 0xFF 0x01) and (0x03 0x00 0xFF 0x00 0x02). The second record will be considered less than 
the first because the fourth byte compares less than, which is incorrect. The root cause is that the 
1-byte terminator cannot be distinguished from the escaped sequence.


Now, escaping and using the two-byte terminator we get:
(0x03 0x00 0x01 0xFF 0x01) and (0x03 0x00 0xFF 0x00 0x01 0x02). Now the records are ordered correctly 
because our 0x01 byte in the terminator compares less than our 0xFF byte in the escape sequence.


The escape sequence also preserves ordering within a field because the un-encoded 0x00 byte must have 
compared less than or equal to any other byte, which is still true after the encoding.


### NULL field encoding

We want to be able to encode NULL field values (here we are discussing whole fields being NULL, not null 
bytes) as either high or low. This becomes complicated especially with variable length fields and NULL last, 
where we need to construct a key that is greater than any non-NULL key.


The solution, also borrowed from CockroachDB, is to encode a type byte as the first part of the field 
encoding. The type byte can range from 0x01 to 0xFE which specifies the type of the following field. If the 
field is NULL and the schema defines it as NULL first, than 0x00 is encoded as the type byte and 
the terminator is appended to it. If the field is NULL and the schema defines it as NULL last, then 0xFF is 
encoded as the type byte and the terminator appended. This way the ordering is always honored.


This approach also helps with decoding support since the type byte can tell us how to interpret the 
following bytes. Combined with having a distinct terminator to determine the length of variable length 
fields, we can have full decoding support. The only exception is that if we have a NULL value we can't 
know what type it was from the key alone.


### Encoding of numeric types

Each specific numeric type we want to support will have its own encoding scheme for within a field. The 
terminator and type encoding will stay the same. The simplest types to support are fixed-size unsigned 
integer types, where we can simply convert to big-endian byte order. The most complicated would be 
signed numerics of variable size which we can use b128 encoding combined with the ELEN methods 
(see the references below). Overall there will be implementation work to support each type but I do not 
forsee any blocking problems.


### Summary

To summarize, the encoding of each field, assuming the value is non-NULL is: 
TYPE + TYPE\_ENCODING + TERMINATOR. Where TYPE is 1 byte, TYPE\_ENCODING is the potentially variable 
length type-specific encoding, and TERMINATOR is the two-byte sequence 0x00 0x01.


Then each of the encoded fields in the key are simply concatenated in the order specifiec by the schema. 
This fulfills all five of our desired properties.


### Implementation Notes

The client library is responsible for encoding the partition and range keys from the record's field values. 
It must do this for the partition key so that the request can be routed, so for simplicity it will also 
do this for the range key. When the server gets a request it concatenates SCHEMA\_NAME + TERMINATOR + 
PARTITION\_KEY + RANGE\_KEY for indexer operations. The server does not need to do further encoding beyond 
this for normal read or write requests. The schema name is required to differentiate records for different 
schemas and to keep records from the same schema together for efficient query operations. It is not part 
of the partition key because the user may want to keep data from different schemas local to a partition.


For query predicates that reference a key field, the client library will also need to encode those 
predicate literals using the same methods it does for keys.

### References

- https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go
- https://www.zanopha.com/docs/elen.pdf
