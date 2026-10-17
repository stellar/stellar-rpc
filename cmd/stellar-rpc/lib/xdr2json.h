#include "shared.h"

typedef struct {
    const char* const json;
    const char* const error;
} conversion_result_t;

conversion_result_t* xdr_to_json(
    const char* const typename,
    xdr_t xdr,
    size_t max_len
);

void free_conversion_result(conversion_result_t*);