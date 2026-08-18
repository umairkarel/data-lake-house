from aiohttp.web import (
    json_response,
)


JSON_TYPE = "application/json"


def success_response(
    data,
    text=None,
    body=None,
    status=200,
    reason=None,
    headers=None,
    content_type=JSON_TYPE,
):
    return json_response(
        data,
        text=text,
        body=body,
        status=status,
        reason=reason,
        headers=headers,
        content_type=content_type,
    )


def failed_response(
    data,
    text=None,
    body=None,
    status=400,
    reason=None,
    headers=None,
    content_type=JSON_TYPE,
):
    return json_response(
        data,
        text=text,
        body=body,
        status=status,
        reason=reason,
        headers=headers,
        content_type=content_type,
    )
