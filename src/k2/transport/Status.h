#pragma once

#include <signal.h>
#include <iostream>

#include <k2/common/Log.h>
#include <k2/transport/PayloadSerialization.h>
namespace k2 {

// A status in K2 follows established HTTP codes. API writers should use the status generators below to produce
// status codes as desired. The code and description are standard HTTP, but a custom message can be also delivered
// with each such code.
struct Status {
    int code;
    String message;
    K2_PAYLOAD_FIELDS(code, message);
    // 1xx series codes for in-progress work
    bool is1xxInProgress() const {return code >=100 && code <=199;}
    // 2xx OK codes
    bool is2xxOK() const {return code >=200 && code <=299;}
    // 3xx action needed codes
    bool is3xxActionNeeded() const {return code >=300 && code <=399;}
    // 4xx non-retryable failure codes
    bool is4xxNonRetryable() const {return code >=400 && code <=499;}
    // 5xx retryable error codes
    bool is5xxRetryable() const {return code >=500 && code <=599;}

    String getDescription() const {
        switch (code) {
            case 100: return "Continue";
            case 101: return "Switching Protocols";
            case 102: return "Processing";
            case 103: return "Early Hints";

            case 200: return "OK";
            case 201: return "Created";
            case 202: return "Accepted";
            case 203: return "Non-Authoritative Information";
            case 204: return "No Content";
            case 205: return "Reset Content";
            case 206: return "Partial Content";
            case 207: return "Multi-Status";
            case 208: return "Already Reported";
            case 218: return "This is fine";
            case 226: return "IM Used";

            case 300: return "Multiple Choices";
            case 301: return "Moved Permanently";
            case 302: return "Found";
            case 303: return "See Other";
            case 304: return "Not Modified";
            case 305: return "Use Proxy";
            case 306: return "Switch Proxy";
            case 307: return "Temporary Redirect";
            case 308: return "Permanent Redirect";

            case 400: return "Bad Request";
            case 401: return "Unauthorized";
            case 402: return "Payment Required";
            case 403: return "Forbidden";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 406: return "Not Acceptable";
            case 407: return "Proxy Authentication Required";
            case 408: return "Request Timeout";
            case 409: return "Conflict";
            case 410: return "Gone";
            case 411: return "Length Required";
            case 412: return "Precondition Failed";
            case 413: return "Payload Too Large";
            case 414: return "URI Too Long";
            case 415: return "Unsupported Media Type";
            case 416: return "Range Not Satisfiable";
            case 417: return "Expectation Failed";
            case 418: return "I'm a teapot";
            case 419: return "Page Expired";
            case 420: return "Enhance Your Calm";
            case 421: return "Misdirected Request";
            case 422: return "Unprocessable Entity";
            case 423: return "Locked";
            case 424: return "Failed Dependency";
            case 425: return "Too Early";
            case 426: return "Upgrade Required";
            case 428: return "Precondition Required";
            case 429: return "Too Many Requests";
            case 440: return "Login Time-out";
            case 444: return "No Response";
            case 449: return "Retry With";
            case 450: return "Blocked by Windows Parental Controls";
            case 451: return "Unavailable For Legal Reasons";
            case 460: return "LB Client Connection Closed";
            case 463: return "LB Request Too Large";
            case 494: return "Request header too large";
            case 495: return "SSL Certificate Error";
            case 496: return "SSL Certificate Required";
            case 497: return "HTTP Request Sent to HTTPS Port";
            case 499: return "Client Closed Request";

            case 500: return "Internal Server Error";
            case 501: return "Not Implemented";
            case 502: return "Bad Gateway";
            case 503: return "Service Unavailable";
            case 504: return "Gateway Timeout";
            case 505: return "HTTP Version Not Supported";
            case 506: return "Variant Also Negotiates";
            case 507: return "Insufficient Storage";
            case 508: return "Loop Detected";
            case 509: return "Bandwidth Limit Exceeded";
            case 510: return "Not Extended";
            case 511: return "Network Authentication Required";
            case 520: return "Web Server Returned an Unknown Error";
            case 521: return "Web Server Is Down";
            case 522: return "Connection Timed Out";
            case 523: return "Origin Is Unreachable";
            case 524: return "A Timeout Occurred";
            case 525: return "SSL Handshake Failed";
            case 526: return "Invalid SSL Certificate";
            case 527: return "Railgun Error";
            case 529: return "Site is overloaded";
            case 598: return "Network read timeout error";

            default: return "Unknown";
        }
    }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 1xx Informational response
// 100 Continue
// The server has received the request headers and the client should proceed to send the request body (in the case of a request for which a body needs to be sent; for example, a POST request). Sending a large request body to a server after a request has been rejected for inappropriate headers would be inefficient. To have a server check the request's headers, a client must send Expect: 100-continue as a header in its initial request and receive a 100 Continue status code in response before sending the body. If the client receives an error code such as 403 (Forbidden) or 405 (Method Not Allowed) then it shouldn't send the request's body. The response 417 Expectation Failed indicates that the request should be repeated without the Expect header as it indicates that the server doesn't support expectations (this is the case, for example, of HTTP/1.0 servers)
static Status S100_Continue(String message="") { return Status{.code = 100, .message = std::move(message)}; }

// 101 Switching Protocols
// The requester has asked the server to switch protocols and the server has agreed to do so.
static Status S101_Switching_Protocols(String message="") { return Status{.code = 101, .message = std::move(message)}; }

// 102 Processing (WebDAV; RFC 2518)
// A WebDAV request may contain many sub-requests involving file operations, requiring a long time to complete the request. This code indicates that the server has received and is processing the request, but no response is available yet. This prevents the client from timing out and assuming the request was lost.
static Status S102_Processing(String message="") { return Status{.code = 102, .message = std::move(message)}; }

// 103 Early Hints (RFC 8297)
// Used to return some response headers before final HTTP message.
static Status S103_Early_Hints(String message="") { return Status{.code = 103, .message = std::move(message)}; }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 2xx Success
// This class of status codes indicates the action requested by the client was received, understood and accepted.

// 200 OK
// Standard response for successful HTTP requests. The actual response will depend on the request method used. In a GET request, the response will contain an entity corresponding to the requested resource. In a POST request, the response will contain an entity describing or containing the result of the action.
static Status S200_OK(String message="") { return Status{.code = 200, .message = std::move(message)}; }

// 201 Created
// The request has been fulfilled, resulting in the creation of a new resource.
static Status S201_Created(String message="") { return Status{.code = 201, .message = std::move(message)}; }

// 202 Accepted
// The request has been accepted for processing, but the processing has not been completed. The request might or might not be eventually acted upon, and may be disallowed when processing occurs.
static Status S202_Accepted(String message="") { return Status{.code = 202, .message = std::move(message)}; }

// 203 Non-Authoritative Information (since HTTP/1.1)
// The server is a transforming proxy(e.g.a Web accelerator) that received a 200 OK from its origin, but is returning a modified version of the origin's response.
static Status S203_Non_Authoritative_Information(String message="") { return Status{.code = 203, .message = std::move(message)}; }

// 204 No Content
// The server successfully processed the request and is not returning any content.
static Status S204_No_Content(String message="") { return Status{.code = 204, .message = std::move(message)}; }

// 205 Reset Content
// The server successfully processed the request, but is not returning any content. Unlike a 204 response, this response requires that the requester reset the document view.
static Status S205_Reset_Content(String message="") { return Status{.code = 205, .message = std::move(message)}; }

// 206 Partial Content (RFC 7233)
// The server is delivering only part of the resource (byte serving) due to a range header sent by the client. The range header is used by HTTP clients to enable resuming of interrupted downloads, or split a download into multiple simultaneous streams.
static Status S206_Partial_Content(String message="") { return Status{.code = 206, .message = std::move(message)}; }

// 207 Multi-Status (WebDAV; RFC 4918)
// The message body that follows is by default an XML message and can contain a number of separate response codes, depending on how many sub-requests were made.
static Status S207_Multi_Status(String message="") { return Status{.code = 207, .message = std::move(message)}; }

// 208 Already Reported (WebDAV; RFC 5842)
// The members of a DAV binding have already been enumerated in a preceding part of the (multistatus) response, and are not being included again.
static Status S208_Already_Reported(String message="") { return Status{.code = 208, .message = std::move(message)}; }

// 218 This is fine (Apache Web Server)
// Used as a catch-all error condition for allowing response bodies to flow through Apache when ProxyErrorOverride is enabled. When ProxyErrorOverride is enabled in Apache, response bodies that contain a status code of 4xx or 5xx are automatically discarded by Apache in favor of a generic response or a custom response specified by the ErrorDocument directive.
static Status S218_This_Is_Fine(String message="") { return Status{.code = 218, .message = std::move(message)}; }

// 226 IM Used (RFC 3229)
// The server has fulfilled a request for the resource, and the response is a representation of the result of one or more instance-manipulations applied to the current instance.
static Status S226_IM_Used(String message="") { return Status{.code = 226, .message = std::move(message)}; }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 3xx Redirection
// This class of status code indicates the client must take additional action to complete the request. Many of these status codes are used in URL redirection.
// A user agent may carry out the additional action with no user interaction only if the method used in the second request is GET or HEAD. A user agent may automatically redirect a request. A user agent should detect and intervene to prevent cyclical redirects.

// 300 Multiple Choices
// Indicates multiple options for the resource from which the client may choose (via agent-driven content negotiation). For example, this code could be used to present multiple video format options, to list files with different filename extensions, or to suggest word-sense disambiguation.
static Status S300_Multiple_Choices(String message="") { return Status{.code = 300, .message = std::move(message)}; }

// 301 Moved Permanently
// This and all future requests should be directed to the given URI.
static Status S301_Moved_Permanently(String message="") { return Status{.code = 301, .message = std::move(message)}; }

// 302 Found (Previously "Moved temporarily")
// Tells the client to look at (browse to) another URL. 302 has been superseded by 303 and 307. This is an example of industry practice contradicting the standard. The HTTP/1.0 specification (RFC 1945) required the client to perform a temporary redirect (the original describing phrase was "Moved Temporarily"), but popular browsers implemented 302 with the functionality of a 303 See Other. Therefore, HTTP/1.1 added status codes 303 and 307 to distinguish between the two behaviours. However, some Web applications and frameworks use the 302 status code as if it were the 303.
static Status S302_Found(String message="") { return Status{.code = 302, .message = std::move(message)}; }

// 303 See Other (since HTTP/1.1)
// The response to the request can be found under another URI using the GET method. When received in response to a POST (or PUT/DELETE), the client should presume that the server has received the data and should issue a new GET request to the given URI.
static Status S303_See_Other(String message="") { return Status{.code = 303, .message = std::move(message)}; }

// 304 Not Modified (RFC 7232)
// Indicates that the resource has not been modified since the version specified by the request headers If-Modified-Since or If-None-Match. In such case, there is no need to retransmit the resource since the client still has a previously-downloaded copy.
static Status S304_Not_Modified(String message="") { return Status{.code = 304, .message = std::move(message)}; }

// 305 Use Proxy (since HTTP/1.1)
// The requested resource is available only through a proxy, the address for which is provided in the response. For security reasons, many HTTP clients (such as Mozilla Firefox and Internet Explorer) do not obey this status code.
static Status S305_Use_Proxy(String message="") { return Status{.code = 305, .message = std::move(message)}; }

// 306 Switch Proxy
// No longer used. Originally meant "Subsequent requests should use the specified proxy."
static Status S306_Switch_Proxy(String message="") { return Status{.code = 306, .message = std::move(message)}; }

// 307 Temporary Redirect (since HTTP/1.1)
// In this case, the request should be repeated with another URI;however, future requests should still use the original URI.In contrast to how 302 was historically implemented, the request method is not allowed to be changed when  reissuing the original request. For example, a POST request should be repeated using another POST request.
static Status S307_Temporary_Redirect(String message="") { return Status{.code = 307, .message = std::move(message)}; }

// 308 Permanent Redirect (RFC 7538)
// The request and all future requests should be repeated using another URI. 307 and 308 parallel the behaviors of 302 and 301, but do not allow the HTTP method to change. So, for example, submitting a form to a permanently redirected resource may continue smoothly.
static Status S308_Permanent_Redirect(String message="") { return Status{.code = 308, .message = std::move(message)}; }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 4xx Client errors
// This class of status code is intended for situations in which the error seems to have been caused by the client. Except when responding to a HEAD request, the server should include an entity containing an explanation of the error situation, and whether it is a temporary or permanent condition. These status codes are applicable to any request method. User agents should display any included entity to the user.

// 400 Bad Request
// The server cannot or will not process the request due to an apparent client error (e.g., malformed request syntax, size too large, invalid request message framing, or deceptive request routing).
static Status S400_Bad_Request(String message="") { return Status{.code = 400, .message = std::move(message)}; }

// 401 Unauthorized (RFC 7235)
// Similar to 403 Forbidden, but specifically for use when authentication is required and has failed or has not yet been provided. The response must include a WWW-Authenticate header field containing a challenge applicable to the requested resource. See Basic access authentication and Digest access authentication. 401 semantically means "unauthorised", the user does not have valid authentication credentials for the target resource.
// Note: Some sites incorrectly issue HTTP 401 when an IP address is banned from the website (usually the website domain) and that specific address is refused permission to access a website.
static Status S401_Unauthorized(String message="") { return Status{.code = 401, .message = std::move(message)}; }

// 402 Payment Required
// Reserved for future use. The original intention was that this code might be used as part of some form of digital cash or micropayment scheme, as proposed, for example, by GNU Taler, but that has not yet happened, and this code is not usually used. Google Developers API uses this status if a particular developer has exceeded the daily limit on requests. Sipgate uses this code if an account does not have sufficient funds to start a call. Shopify uses this code when the store has not paid their fees and is temporarily disabled. Stripe uses this code for failed payments where parameters were correct, for example blocked fraudulent payments.
static Status S402_Payment_Required(String message="") { return Status{.code = 402, .message = std::move(message)}; }

// 403 Forbidden
// The request contained valid data and was understood by the server, but the server is refusing action. This may be due to the user not having the necessary permissions for a resource or needing an account of some sort, or attempting a prohibited action (e.g. creating a duplicate record where only one is allowed). This code is also typically used if the request provided authentication via the WWW-Authenticate header field, but the server did not accept that authentication. The request should not be repeated.
static Status S403_Forbidden(String message="") { return Status{.code = 403, .message = std::move(message)}; }

// 404 Not Found
// The requested resource could not be found but may be available in the future. Subsequent requests by the client are permissible.
static Status S404_Not_Found(String message="") { return Status{.code = 404, .message = std::move(message)}; }

// 405 Method Not Allowed
// A request method is not supported for the requested resource; for example, a GET request on a form that requires data to be presented via POST, or a PUT request on a read-only resource.
static Status S405_Method_Not_Allowed(String message="") { return Status{.code = 405, .message = std::move(message)}; }

// 406 Not Acceptable
// The requested resource is capable of generating only content not acceptable according to the Accept headers sent in the request. See Content negotiation.
static Status S406_Not_Acceptable(String message="") { return Status{.code = 406, .message = std::move(message)}; }

// 407 Proxy Authentication Required (RFC 7235)
// The client must first authenticate itself with the proxy.
static Status S407_Proxy_Authentication_Required(String message="") { return Status{.code = 407, .message = std::move(message)}; }

// 408 Request Timeout
// The server timed out waiting for the request. According to HTTP specifications: "The client did not produce a request within the time that the server was prepared to wait. The client MAY repeat the request without modifications at any later time."
static Status S408_Request_Timeout(String message="") { return Status{.code = 408, .message = std::move(message)}; }

// 409 Conflict
// Indicates that the request could not be processed because of conflict in the current state of the resource, such as an edit conflict between multiple simultaneous updates.
static Status S409_Conflict(String message="") {return Status{.code = 409, .message = std::move(message)}; }

// 410 Gone
// Indicates that the resource requested is no longer available and will not be available again. This should be used when a resource has been intentionally removed and the resource should be purged. Upon receiving a 410 status code, the client should not request the resource in the future. Clients such as search engines should remove the resource from their indices. Most use cases do not require clients and search engines to purge the resource, and a "404 Not Found" may be used instead.
static Status S410_Gone(String message="") { return Status{.code = 410, .message = std::move(message)}; }

// 411 Length Required
// The request did not specify the length of its content, which is required by the requested resource.
static Status S411_Length_Required(String message="") {return Status{.code = 411, .message = std::move(message)}; }

// 412 Precondition Failed (RFC 7232)
// The server does not meet one of the preconditions that the requester put on the request header fields.
static Status S412_Precondition_Failed(String message="") { return Status{.code = 412, .message = std::move(message)}; }

// 413 Payload Too Large (RFC 7231)
// The request is larger than the server is willing or able to process. Previously called "Request Entity Too Large".
static Status S413_Payload_Too_Large(String message="") { return Status{.code = 413, .message = std::move(message)}; }

// 414 URI Too Long (RFC 7231)
// The URI provided was too long for the server to process. Often the result of too much data being encoded as a query-string of a GET request, in which case it should be converted to a POST request. Called "Request-URI Too Long" previously.
static Status S414_URI_Too_Long(String message="") { return Status{.code = 414, .message = std::move(message)}; }

// 415 Unsupported Media Type (RFC 7231)
// The request entity has a media type which the server or resource does not support. For example, the client uploads an image as image/svg+xml, but the server requires that images use a different format.
static Status S415_Unsupported_Media_Type(String message="") { return Status{.code = 415, .message = std::move(message)}; }

// 416 Range Not Satisfiable (RFC 7233)
// The client has asked for a portion of the file (byte serving), but the server cannot supply that portion. For example, if the client asked for a part of the file that lies beyond the end of the file. Called "Requested Range Not Satisfiable" previously.
static Status S416_Range_Not_Satisfiable(String message="") { return Status{.code = 416, .message = std::move(message)}; }

// 417 Expectation Failed
// The server cannot meet the requirements of the Expect request-header field.
static Status S417_Expectation_Failed(String message="") { return Status{.code = 417, .message = std::move(message)}; }

// 418 I'm a teapot (RFC 2324, RFC 7168)
// This code was defined in 1998 as one of the traditional IETF April Fools' jokes, in RFC 2324, Hyper Text Coffee Pot Control Protocol, and is not expected to be implemented by actual HTTP servers. The RFC specifies this code should be returned by teapots requested to brew coffee. This HTTP status is used as an Easter egg in some websites, including Google.com.
static Status S418_Im_a_teapot(String message="") { return Status{.code = 418, .message = std::move(message)}; }

// 419 Page Expired (Laravel Framework)
// Used by the Laravel Framework when a CSRF Token is missing or expired.
static Status S419_Page_Expired(String message="") { return Status{.code = 419, .message = std::move(message)};}

// 420 Enhance Your Calm (Twitter)
// Returned by version 1 of the Twitter Search and Trends API when the client is being rate limited; versions 1.1 and later use the 429 Too Many Requests response code instead.
static Status S420_Enhance_Your_Calm(String message="") { return Status{.code = 420, .message = std::move(message)}; }

// 421 Misdirected Request (RFC 7540)
// The request was directed at a server that is not able to produce a response (for example because of connection reuse).
static Status S421_Misdirected_Request(String message="") { return Status{.code = 421, .message = std::move(message)}; }

// 422 Unprocessable Entity (WebDAV; RFC 4918)
// The request was well-formed but was unable to be followed due to semantic errors.
static Status S422_Unprocessable_Entity(String message="") { return Status{.code = 422, .message = std::move(message)}; }

// 423 Locked (WebDAV; RFC 4918)
// The resource that is being accessed is locked.
static Status S423_Locked(String message="") { return Status{.code = 423, .message = std::move(message)}; }

// 424 Failed Dependency (WebDAV; RFC 4918)
// The request failed because it depended on another request and that request failed (e.g., a PROPPATCH).
static Status S424_Failed_Dependency(String message="") { return Status{.code = 424, .message = std::move(message)}; }

// 425 Too Early (RFC 8470)
// Indicates that the server is unwilling to risk processing a request that might be replayed.
static Status S425_Too_Early(String message="") { return Status{.code = 425, .message = std::move(message)}; }

// 426 Upgrade Required
// The client should switch to a different protocol such as TLS/1.0, given in the Upgrade header field.
static Status S426_Upgrade_Required(String message="") { return Status{.code = 426, .message = std::move(message)}; }

// 428 Precondition Required (RFC 6585)
// The origin server requires the request to be conditional. Intended to prevent the 'lost update' problem, where a client GETs a resource's state, modifies it, and PUTs it back to the server, when meanwhile a third party has modified the state on the server, leading to a conflict.
static Status S428_Precondition_Required(String message="") { return Status{.code = 428, .message = std::move(message)}; }

// 429 Too Many Requests (RFC 6585)
// The user has sent too many requests in a given amount of time. Intended for use with rate-limiting schemes.
static Status S429_Too_Many_Requests(String message="") { return Status{.code = 429, .message = std::move(message)}; }

// 440 Login Time-out
// MS IIS extension
// The client's session has expired and must log in again.
static Status S440_Login_Timeout(String message="") { return Status{.code = 440, .message = std::move(message)}; }

// 444 No Response
// nginx extension
// Used internally to instruct the server to return no information to the client and close the connection immediately.
static Status S444_No_Response(String message="") { return Status{.code = 444, .message = std::move(message)}; }

// 449 Retry With
// MS IIS extension
// The server cannot honour the request because the user has not provided the required information.
static Status S449_Retry_With(String message="") { return Status{.code = 449, .message = std::move(message)}; }

// 450 Blocked by Windows Parental Controls (Microsoft)
// The Microsoft extension code indicated when Windows Parental Controls are turned on and are blocking access to the requested webpage.
static Status S450_Blocked_by_Windows_Parental_Controls(String message="") { return Status{.code = 450, .message = std::move(message)}; }

// 451 Unavailable For Legal Reasons (RFC 7725)
// A server operator has received a legal demand to deny access to a resource or to a set of resources that includes the requested resource. The code 451 was chosen as a reference to the novel Fahrenheit 451 (see the Acknowledgements in the RFC).
static Status S451_Unavailable_For_Legal_Reasons(String message="") { return Status{.code = 451, .message = std::move(message)}; }

// 460 LB Client Connection Closed
// AWS Elastic Load Balancer
// Client closed the connection with the load balancer before the idle timeout period elapsed. Typically when client timeout is sooner than the Elastic Load Balancer's timeout.
static Status S460_LB_Client_Connection_Closed(String message="") { return Status{.code = 460, .message = std::move(message)}; }

// 463 LB Request Too Large
// AWS Elastic Load Balancer
// The load balancer received an X-Forwarded-For request header with more than 30 IP addresses.
static Status S463_LB_Request_Too_Large(String message="") { return Status{.code = 463, .message = std::move(message)}; }

// 494 Request header too large
// nginx extension
// Client sent too large request or too long header line.
static Status S494_Request_header_too_large(String message="") { return Status{.code = 494, .message = std::move(message)}; }

// 495 SSL Certificate Error
// nginx extension
// An expansion of the 400 Bad Request response code, used when the client has provided an invalid client certificate.
static Status S495_SSL_Certificate_Error(String message="") { return Status{.code = 495, .message = std::move(message)}; }

// 496 SSL Certificate Required
// nginx extension
// An expansion of the 400 Bad Request response code, used when a client certificate is required but not provided.
static Status S496_SSL_Certificate_Required(String message="") { return Status{.code = 496, .message = std::move(message)}; }

// 497 HTTP Request Sent to HTTPS Port
// nginx extension
// An expansion of the 400 Bad Request response code, used when the client has made a HTTP request to a port listening for HTTPS requests.
static Status S497_HTTP_Request_Sent_to_HTTPS_Port(String message="") { return Status{.code = 497, .message = std::move(message)}; }

// 499 Client Closed Request
// nginx extension
// Used when the client has closed the request before the server could send a response.
static Status S499_Client_Closed_Request(String message="") { return Status{.code = 499, .message = std::move(message)}; }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 5xx Server errors
// The server failed to fulfill a request.
//
// Response status codes beginning with the digit "5" indicate cases in which the server is aware that it has encountered an error or is otherwise incapable of performing the request. Except when responding to a HEAD request, the server should include an entity containing an explanation of the error situation, and indicate whether it is a temporary or permanent condition. Likewise, user agents should display any included entity to the user. These response codes are applicable to any request method.

// 500 Internal Server Error
// A generic error message, given when an unexpected condition was encountered and no more specific message is suitable.
static Status S500_Internal_Server_Error(String message="") { return Status{.code = 500, .message = std::move(message)}; }

// 501 Not Implemented
// The server either does not recognize the request method, or it lacks the ability to fulfil the request. Usually this implies future availability (e.g., a new feature of a web-service API).
static Status S501_Not_Implemented(String message="") { return Status{.code = 501, .message = std::move(message)}; }

// 502 Bad Gateway
// The server was acting as a gateway or proxy and received an invalid response from the upstream server.
static Status S502_Bad_Gateway(String message="") { return Status{.code = 502, .message = std::move(message)}; }

// 503 Service Unavailable
// The server cannot handle the request (because it is overloaded or down for maintenance). Generally, this is a temporary state.
static Status S503_Service_Unavailable(String message="") { return Status{.code = 503, .message = std::move(message)}; }

// 504 Gateway Timeout
// The server was acting as a gateway or proxy and did not receive a timely response from the upstream server.
static Status S504_Gateway_Timeout(String message="") { return Status{.code = 504, .message = std::move(message)}; }

// 505 HTTP Version Not Supported
// The server does not support the HTTP protocol version used in the request.
static Status S505_HTTP_Version_Not_Supported(String message="") { return Status{.code = 505, .message = std::move(message)}; }

// 506 Variant Also Negotiates (RFC 2295)
// Transparent content negotiation for the request results in a circular reference.
static Status S506_Variant_Also_Negotiates(String message="") { return Status{.code = 506, .message = std::move(message)}; }

// 507 Insufficient Storage (WebDAV; RFC 4918)
// The server is unable to store the representation needed to complete the request.
static Status S507_Insufficient_Storage(String message="") { return Status{.code = 507, .message = std::move(message)}; }

// 508 Loop Detected (WebDAV; RFC 5842)
// The server detected an infinite loop while processing the request (sent instead of 208 Already Reported).
static Status S508_Loop_Detected(String message="") { return Status{.code = 508, .message = std::move(message)}; }

// 509 Bandwidth Limit Exceeded (Apache Web Server/cPanel)
// The server has exceeded the bandwidth specified by the server administrator; this is often used by shared hosting providers to limit the bandwidth of customers.
static Status S509_Bandwidth_Limit_Exceeded(String message="") { return Status{.code = 509, .message = std::move(message)}; }

// 510 Not Extended (RFC 2774)
// Further extensions to the request are required for the server to fulfil it.
static Status S510_Not_Extended(String message="") { return Status{.code = 510, .message = std::move(message)}; }

// 511 Network Authentication Required (RFC 6585)
// The client needs to authenticate to gain network access. Intended for use by intercepting proxies used to control access to the network (e.g., "captive portals" used to require agreement to Terms of Service before granting full Internet access via a Wi-Fi hotspot).
static Status S511_Network_Authentication_Required(String message="") { return Status{.code = 511, .message = std::move(message)}; }

// 520 Web Server Returned an Unknown Error
// Cloudflare extension
// The origin server returned an empty, unknown, or unexplained response to Cloudflare.
static Status S520_Web_Server_Returned_an_Unknown_Error(String message="") { return Status{.code = 520, .message = std::move(message)}; }

// 521 Web Server Is Down
// Cloudflare extension
// The origin server has refused the connection from Cloudflare.
static Status S521_Web_Server_Is_Down(String message="") { return Status{.code = 521, .message = std::move(message)}; }

// 522 Connection Timed Out
// Cloudflare extension
// Cloudflare could not negotiate a TCP handshake with the origin server.
static Status S522_Connection_Timed_Out(String message="") { return Status{.code = 522, .message = std::move(message)}; }

// 523 Origin Is Unreachable
// Cloudflare extension
// Cloudflare could not reach the origin server; for example, if the DNS records for the origin server are incorrect.
static Status S523_Origin_Is_Unreachable(String message="") { return Status{.code = 523, .message = std::move(message)}; }

// 524 A Timeout Occurred
// Cloudflare extension
// Cloudflare was able to complete a TCP connection to the origin server, but did not receive a timely HTTP response.
static Status S524_A_Timeout_Occurred(String message="") { return Status{.code = 524, .message = std::move(message)}; }

// 525 SSL Handshake Failed
// Cloudflare extension
// Cloudflare could not negotiate a SSL/TLS handshake with the origin server.
static Status S525_SSL_Handshake_Failed(String message="") { return Status{.code = 525, .message = std::move(message)}; }

// 526 Invalid SSL Certificate
// Cloudflare extension
// Cloudflare could not validate the SSL certificate on the origin web server.
static Status S526_Invalid_SSL_Certificate(String message="") { return Status{.code = 526, .message = std::move(message)}; }

// 527 Railgun Error
// Cloudflare extension
// Error 527 indicates an interrupted connection between Cloudflare and the origin server's Railgun server.
static Status S527_Railgun_Error(String message="") { return Status{.code = 527, .message = std::move(message)}; }

// 529 Site is overloaded
// Used by Qualys in the SSLLabs server testing API to signal that the site can't process the request.
static Status S529_Site_is_overloaded(String message="") { return Status{.code = 529, .message = std::move(message)}; }

// 598 (Informal convention) Network read timeout error
// Used by some HTTP proxies to signal a network read timeout behind the proxy to a client in front of the proxy.
static Status S598_Network_read_timeout_error(String message="") { return Status{.code = 598, .message = std::move(message)}; }

}; // struct status

inline std::ostream& operator<<(std::ostream& os, const Status& st) {
    os << "[" << st.code << " " << st.getDescription() << "]: " << st.message;
    return os;
}

} //namespace k2

#define RET_IF_BAD(status)                                     \
    {                                                          \
        k2::Status ____status____ = std::move((status));       \
        if (!____status____.is2xxOK()) return ____status____; \
    }
#define THROW_IF_BAD(status)                  \
    {                                         \
        k2::Status ____status____ = std::move((status)); \
        if (!____status____.is2xxOK()) {     \
            throw ____status____;             \
        }                                     \
    }
