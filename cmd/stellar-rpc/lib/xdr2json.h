#include "shared.h"

typedef struct {
    const char* const json;
    const char* const error;
} conversion_result_t;

conversion_result_t* xdr_to_json(
    const char* const typename,
    xdr_t xdr
);

void free_conversion_result(conversion_result_t*);

typedef struct {
    xdr_t xdr;
    const char* const error;
} json_to_xdr_result_t;

json_to_xdr_result_t* json_to_xdr(
    const char* const typename,
    xdr_t json
);

void free_json_to_xdr_result(json_to_xdr_result_t*);