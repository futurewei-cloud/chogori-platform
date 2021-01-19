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
    // two Statuses are equal if they have the same code
    bool operator==(const Status& o);
    bool operator!=(const Status& o);

    // Create a new status object with different message but same code. This is useful for the staticly
    // defined codes below
    Status operator()(String message) const;

    // 1xx series codes for in-progress work
    bool is1xxInProgress() const;
    // 2xx OK codes
    bool is2xxOK() const;
    // 3xx action needed codes
    bool is3xxActionNeeded() const;
    // 4xx non-retryable failure codes
    bool is4xxNonRetryable() const;
    // 5xx retryable error codes
    bool is5xxRetryable() const;

    String getDescription() const;

    K2_DEF_FMT(Status, code, message);
};


struct Statuses {
/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 1xx Informational response
// 100 Continue
// The server has received the request headers and the client should proceed to send the request body (in the case of a request for which a body needs to be sent; for example, a POST request). Sending a large request body to a server after a request has been rejected for inappropriate headers would be inefficient. To have a server check the request's headers, a client must send Expect: 100-continue as a header in its initial request and receive a 100 Continue status code in response before sending the body. If the client receives an error code such as 403 (Forbidden) or 405 (Method Not Allowed) then it shouldn't send the request's body. The response 417 Expectation Failed indicates that the request should be repeated without the Expect header as it indicates that the server doesn't support expectations (this is the case, for example, of HTTP/1.0 servers)
static const inline Status S100_Continue{.code=100, .message=""};

// 101 Switching Protocols
// The requester has asked the server to switch protocols and the server has agreed to do so.
static const inline Status S101_Switching_Protocols{.code=101, .message=""};

// 102 Processing (WebDAV; RFC 2518)
// A WebDAV request may contain many sub-requests involving file operations, requiring a long time to complete the request. This code indicates that the server has received and is processing the request, but no response is available yet. This prevents the client from timing out and assuming the request was lost.
static const inline Status S102_Processing{.code=102, .message=""};

// 103 Early Hints (RFC 8297)
// Used to return some response headers before final HTTP message.
static const inline Status S103_Early_Hints{.code=103, .message=""};
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 2xx Success
// This class of status codes indicates the action requested by the client was received, understood and accepted.

// 200 OK
// Standard response for successful HTTP requests. The actual response will depend on the request method used. In a GET request, the response will contain an entity corresponding to the requested resource. In a POST request, the response will contain an entity describing or containing the result of the action.
static const inline Status S200_OK{.code=200, .message=""};

// 201 Created
// The request has been fulfilled, resulting in the creation of a new resource.
static const inline Status S201_Created{.code=201, .message=""};

// 202 Accepted
// The request has been accepted for processing, but the processing has not been completed. The request might or might not be eventually acted upon, and may be disallowed when processing occurs.
static const inline Status S202_Accepted{.code=202, .message=""};

// 203 Non-Authoritative Information (since HTTP/1.1)
// The server is a transforming proxy(e.g.a Web accelerator) that received a 200 OK from its origin, but is returning a modified version of the origin's response.
static const inline Status S203_Non_Authoritative_Information{.code=203, .message=""};

// 204 No Content
// The server successfully processed the request and is not returning any content.
static const inline Status S204_No_Content{.code=204, .message=""};

// 205 Reset Content
// The server successfully processed the request, but is not returning any content. Unlike a 204 response, this response requires that the requester reset the document view.
static const inline Status S205_Reset_Content{.code=205, .message=""};

// 206 Partial Content (RFC 7233)
// The server is delivering only part of the resource (byte serving) due to a range header sent by the client. The range header is used by HTTP clients to enable resuming of interrupted downloads, or split a download into multiple simultaneous streams.
static const inline Status S206_Partial_Content{.code=206, .message=""};

// 207 Multi-Status (WebDAV; RFC 4918)
// The message body that follows is by default an XML message and can contain a number of separate response codes, depending on how many sub-requests were made.
static const inline Status S207_Multi_Status{.code=207, .message=""};

// 208 Already Reported (WebDAV; RFC 5842)
// The members of a DAV binding have already been enumerated in a preceding part of the (multistatus) response, and are not being included again.
static const inline Status S208_Already_Reported{.code=208, .message=""};

// 218 This is fine (Apache Web Server)
// Used as a catch-all error condition for allowing response bodies to flow through Apache when ProxyErrorOverride is enabled. When ProxyErrorOverride is enabled in Apache, response bodies that contain a status code of 4xx or 5xx are automatically discarded by Apache in favor of a generic response or a custom response specified by the ErrorDocument directive.
static const inline Status S218_This_Is_Fine{.code=218, .message=""};

// 226 IM Used (RFC 3229)
// The server has fulfilled a request for the resource, and the response is a representation of the result of one or more instance-manipulations applied to the current instance.
static const inline Status S226_IM_Used{.code=226, .message=""};
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 3xx Redirection
// This class of status code indicates the client must take additional action to complete the request. Many of these status codes are used in URL redirection.
// A user agent may carry out the additional action with no user interaction only if the method used in the second request is GET or HEAD. A user agent may automatically redirect a request. A user agent should detect and intervene to prevent cyclical redirects.

// 300 Multiple Choices
// Indicates multiple options for the resource from which the client may choose (via agent-driven content negotiation). For example, this code could be used to present multiple video format options, to list files with different filename extensions, or to suggest word-sense disambiguation.
static const inline Status S300_Multiple_Choices{.code=300, .message=""};

// 301 Moved Permanently
// This and all future requests should be directed to the given URI.
static const inline Status S301_Moved_Permanently{.code=301, .message=""};

// 302 Found (Previously "Moved temporarily")
// Tells the client to look at (browse to) another URL. 302 has been superseded by 303 and 307. This is an example of industry practice contradicting the standard. The HTTP/1.0 specification (RFC 1945) required the client to perform a temporary redirect (the original describing phrase was "Moved Temporarily"), but popular browsers implemented 302 with the functionality of a 303 See Other. Therefore, HTTP/1.1 added status codes 303 and 307 to distinguish between the two behaviours. However, some Web applications and frameworks use the 302 status code as if it were the 303.
static const inline Status S302_Found{.code=302, .message=""};

// 303 See Other (since HTTP/1.1)
// The response to the request can be found under another URI using the GET method. When received in response to a POST (or PUT/DELETE), the client should presume that the server has received the data and should issue a new GET request to the given URI.
static const inline Status S303_See_Other{.code=303, .message=""};

// 304 Not Modified (RFC 7232)
// Indicates that the resource has not been modified since the version specified by the request headers If-Modified-Since or If-None-Match. In such case, there is no need to retransmit the resource since the client still has a previously-downloaded copy.
static const inline Status S304_Not_Modified{.code=304, .message=""};

// 305 Use Proxy (since HTTP/1.1)
// The requested resource is available only through a proxy, the address for which is provided in the response. For security reasons, many HTTP clients (such as Mozilla Firefox and Internet Explorer) do not obey this status code.
static const inline Status S305_Use_Proxy{.code=305, .message=""};

// 306 Switch Proxy
// No longer used. Originally meant "Subsequent requests should use the specified proxy."
static const inline Status S306_Switch_Proxy{.code=306, .message=""};

// 307 Temporary Redirect (since HTTP/1.1)
// In this case, the request should be repeated with another URI;however, future requests should still use the original URI.In contrast to how 302 was historically implemented, the request method is not allowed to be changed when  reissuing the original request. For example, a POST request should be repeated using another POST request.
static const inline Status S307_Temporary_Redirect{.code=307, .message=""};

// 308 Permanent Redirect (RFC 7538)
// The request and all future requests should be repeated using another URI. 307 and 308 parallel the behaviors of 302 and 301, but do not allow the HTTP method to change. So, for example, submitting a form to a permanently redirected resource may continue smoothly.
static const inline Status S308_Permanent_Redirect{.code=308, .message=""};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 4xx Client errors
// This class of status code is intended for situations in which the error seems to have been caused by the client. Except when responding to a HEAD request, the server should include an entity containing an explanation of the error situation, and whether it is a temporary or permanent condition. These status codes are applicable to any request method. User agents should display any included entity to the user.

// 400 Bad Request
// The server cannot or will not process the request due to an apparent client error (e.g., malformed request syntax, size too large, invalid request message framing, or deceptive request routing).
static const inline Status S400_Bad_Request{.code=400, .message=""};

// 401 Unauthorized (RFC 7235)
// Similar to 403 Forbidden, but specifically for use when authentication is required and has failed or has not yet been provided. The response must include a WWW-Authenticate header field containing a challenge applicable to the requested resource. See Basic access authentication and Digest access authentication. 401 semantically means "unauthorised", the user does not have valid authentication credentials for the target resource.
// Note: Some sites incorrectly issue HTTP 401 when an IP address is banned from the website (usually the website domain) and that specific address is refused permission to access a website.
static const inline Status S401_Unauthorized{.code=401, .message=""};

// 402 Payment Required
// Reserved for future use. The original intention was that this code might be used as part of some form of digital cash or micropayment scheme, as proposed, for example, by GNU Taler, but that has not yet happened, and this code is not usually used. Google Developers API uses this status if a particular developer has exceeded the daily limit on requests. Sipgate uses this code if an account does not have sufficient funds to start a call. Shopify uses this code when the store has not paid their fees and is temporarily disabled. Stripe uses this code for failed payments where parameters were correct, for example blocked fraudulent payments.
static const inline Status S402_Payment_Required{.code=402, .message=""};

// 403 Forbidden
// The request contained valid data and was understood by the server, but the server is refusing action. This may be due to the user not having the necessary permissions for a resource or needing an account of some sort, or attempting a prohibited action (e.g. creating a duplicate record where only one is allowed). This code is also typically used if the request provided authentication via the WWW-Authenticate header field, but the server did not accept that authentication. The request should not be repeated.
static const inline Status S403_Forbidden{.code=403, .message=""};

// 404 Not Found
// The requested resource could not be found but may be available in the future. Subsequent requests by the client are permissible.
static const inline Status S404_Not_Found{.code=404, .message=""};

// 405 Method Not Allowed
// A request method is not supported for the requested resource; for example, a GET request on a form that requires data to be presented via POST, or a PUT request on a read-only resource.
static const inline Status S405_Method_Not_Allowed{.code=405, .message=""};

// 406 Not Acceptable
// The requested resource is capable of generating only content not acceptable according to the Accept headers sent in the request. See Content negotiation.
static const inline Status S406_Not_Acceptable{.code=406, .message=""};

// 407 Proxy Authentication Required (RFC 7235)
// The client must first authenticate itself with the proxy.
static const inline Status S407_Proxy_Authentication_Required{.code=407, .message=""};

// 408 Request Timeout
// The server timed out waiting for the request. According to HTTP specifications: "The client did not produce a request within the time that the server was prepared to wait. The client MAY repeat the request without modifications at any later time."
static const inline Status S408_Request_Timeout{.code=408, .message=""};

// 409 Conflict
// Indicates that the request could not be processed because of conflict in the current state of the resource, such as an edit conflict between multiple simultaneous updates.
static const inline Status S409_Conflict{.code=409, .message=""};

// 410 Gone
// Indicates that the resource requested is no longer available and will not be available again. This should be used when a resource has been intentionally removed and the resource should be purged. Upon receiving a 410 status code, the client should not request the resource in the future. Clients such as search engines should remove the resource from their indices. Most use cases do not require clients and search engines to purge the resource, and a "404 Not Found" may be used instead.
static const inline Status S410_Gone{.code=410, .message=""};

// 411 Length Required
// The request did not specify the length of its content, which is required by the requested resource.
static const inline Status S411_Length_Required{.code=411, .message=""};

// 412 Precondition Failed (RFC 7232)
// The server does not meet one of the preconditions that the requester put on the request header fields.
static const inline Status S412_Precondition_Failed{.code=412, .message=""};

// 413 Payload Too Large (RFC 7231)
// The request is larger than the server is willing or able to process. Previously called "Request Entity Too Large".
static const inline Status S413_Payload_Too_Large{.code=413, .message=""};

// 414 URI Too Long (RFC 7231)
// The URI provided was too long for the server to process. Often the result of too much data being encoded as a query-string of a GET request, in which case it should be converted to a POST request. Called "Request-URI Too Long" previously.
static const inline Status S414_URI_Too_Long{.code=414, .message=""};

// 415 Unsupported Media Type (RFC 7231)
// The request entity has a media type which the server or resource does not support. For example, the client uploads an image as image/svg+xml, but the server requires that images use a different format.
static const inline Status S415_Unsupported_Media_Type{.code=415, .message=""};

// 416 Range Not Satisfiable (RFC 7233)
// The client has asked for a portion of the file (byte serving), but the server cannot supply that portion. For example, if the client asked for a part of the file that lies beyond the end of the file. Called "Requested Range Not Satisfiable" previously.
static const inline Status S416_Range_Not_Satisfiable{.code=416, .message=""};

// 417 Expectation Failed
// The server cannot meet the requirements of the Expect request-header field.
static const inline Status S417_Expectation_Failed{.code=417, .message=""};

// 418 I'm a teapot (RFC 2324, RFC 7168)
// This code was defined in 1998 as one of the traditional IETF April Fools' jokes, in RFC 2324, Hyper Text Coffee Pot Control Protocol, and is not expected to be implemented by actual HTTP servers. The RFC specifies this code should be returned by teapots requested to brew coffee. This HTTP status is used as an Easter egg in some websites, including Google.com.
static const inline Status S418_Im_a_teapot{.code=418, .message=""};

// 419 Page Expired (Laravel Framework)
// Used by the Laravel Framework when a CSRF Token is missing or expired.
static const inline Status S419_Page_Expired{.code=419, .message=""};

// 420 Enhance Your Calm (Twitter)
// Returned by version 1 of the Twitter Search and Trends API when the client is being rate limited; versions 1.1 and later use the 429 Too Many Requests response code instead.
static const inline Status S420_Enhance_Your_Calm{.code=420, .message=""};

// 421 Misdirected Request (RFC 7540)
// The request was directed at a server that is not able to produce a response (for example because of connection reuse).
static const inline Status S421_Misdirected_Request{.code=421, .message=""};

// 422 Unprocessable Entity (WebDAV; RFC 4918)
// The request was well-formed but was unable to be followed due to semantic errors.
static const inline Status S422_Unprocessable_Entity{.code=422, .message=""};

// 423 Locked (WebDAV; RFC 4918)
// The resource that is being accessed is locked.
static const inline Status S423_Locked{.code=423, .message=""};

// 424 Failed Dependency (WebDAV; RFC 4918)
// The request failed because it depended on another request and that request failed (e.g., a PROPPATCH).
static const inline Status S424_Failed_Dependency{.code=424, .message=""};

// 425 Too Early (RFC 8470)
// Indicates that the server is unwilling to risk processing a request that might be replayed.
static const inline Status S425_Too_Early{.code=425, .message=""};

// 426 Upgrade Required
// The client should switch to a different protocol such as TLS/1.0, given in the Upgrade header field.
static const inline Status S426_Upgrade_Required{.code=426, .message=""};

// 428 Precondition Required (RFC 6585)
// The origin server requires the request to be conditional. Intended to prevent the 'lost update' problem, where a client GETs a resource's state, modifies it, and PUTs it back to the server, when meanwhile a third party has modified the state on the server, leading to a conflict.
static const inline Status S428_Precondition_Required{.code=428, .message=""};

// 429 Too Many Requests (RFC 6585)
// The user has sent too many requests in a given amount of time. Intended for use with rate-limiting schemes.
static const inline Status S429_Too_Many_Requests{.code=429, .message=""};

// 440 Login Time-out
// MS IIS extension
// The client's session has expired and must log in again.
static const inline Status S440_Login_Timeout{.code=440, .message=""};

// 444 No Response
// nginx extension
// Used internally to instruct the server to return no information to the client and close the connection immediately.
static const inline Status S444_No_Response{.code=444, .message=""};

// 449 Retry With
// MS IIS extension
// The server cannot honour the request because the user has not provided the required information.
static const inline Status S449_Retry_With{.code=449, .message=""};

// 450 Blocked by Windows Parental Controls (Microsoft)
// The Microsoft extension code indicated when Windows Parental Controls are turned on and are blocking access to the requested webpage.
static const inline Status S450_Blocked_by_Windows_Parental_Controls{.code=450, .message=""};

// 451 Unavailable For Legal Reasons (RFC 7725)
// A server operator has received a legal demand to deny access to a resource or to a set of resources that includes the requested resource. The code 451 was chosen as a reference to the novel Fahrenheit 451 (see the Acknowledgements in the RFC).
static const inline Status S451_Unavailable_For_Legal_Reasons{.code=451, .message=""};

// 460 LB Client Connection Closed
// AWS Elastic Load Balancer
// Client closed the connection with the load balancer before the idle timeout period elapsed. Typically when client timeout is sooner than the Elastic Load Balancer's timeout.
static const inline Status S460_LB_Client_Connection_Closed{.code=460, .message=""};

// 463 LB Request Too Large
// AWS Elastic Load Balancer
// The load balancer received an X-Forwarded-For request header with more than 30 IP addresses.
static const inline Status S463_LB_Request_Too_Large{.code=463, .message=""};

// 494 Request header too large
// nginx extension
// Client sent too large request or too long header line.
static const inline Status S494_Request_header_too_large{.code=494, .message=""};

// 495 SSL Certificate Error
// nginx extension
// An expansion of the 400 Bad Request response code, used when the client has provided an invalid client certificate.
static const inline Status S495_SSL_Certificate_Error{.code=495, .message=""};

// 496 SSL Certificate Required
// nginx extension
// An expansion of the 400 Bad Request response code, used when a client certificate is required but not provided.
static const inline Status S496_SSL_Certificate_Required{.code=496, .message=""};

// 497 HTTP Request Sent to HTTPS Port
// nginx extension
// An expansion of the 400 Bad Request response code, used when the client has made a HTTP request to a port listening for HTTPS requests.
static const inline Status S497_HTTP_Request_Sent_to_HTTPS_Port{.code=497, .message=""};

// 499 Client Closed Request
// nginx extension
// Used when the client has closed the request before the server could send a response.
static const inline Status S499_Client_Closed_Request{.code=499, .message=""};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 5xx Server errors
// The server failed to fulfill a request.
//
// Response status codes beginning with the digit "5" indicate cases in which the server is aware that it has encountered an error or is otherwise incapable of performing the request. Except when responding to a HEAD request, the server should include an entity containing an explanation of the error situation, and indicate whether it is a temporary or permanent condition. Likewise, user agents should display any included entity to the user. These response codes are applicable to any request method.

// 500 Internal Server Error
// A generic error message, given when an unexpected condition was encountered and no more specific message is suitable.
static const inline Status S500_Internal_Server_Error{.code=500, .message=""};

// 501 Not Implemented
// The server either does not recognize the request method, or it lacks the ability to fulfil the request. Usually this implies future availability (e.g., a new feature of a web-service API).
static const inline Status S501_Not_Implemented{.code=501, .message=""};

// 502 Bad Gateway
// The server was acting as a gateway or proxy and received an invalid response from the upstream server.
static const inline Status S502_Bad_Gateway{.code=502, .message=""};

// 503 Service Unavailable
// The server cannot handle the request (because it is overloaded or down for maintenance). Generally, this is a temporary state.
static const inline Status S503_Service_Unavailable{.code=503, .message=""};

// 504 Gateway Timeout
// The server was acting as a gateway or proxy and did not receive a timely response from the upstream server.
static const inline Status S504_Gateway_Timeout{.code=504, .message=""};

// 505 HTTP Version Not Supported
// The server does not support the HTTP protocol version used in the request.
static const inline Status S505_HTTP_Version_Not_Supported{.code=505, .message=""};

// 506 Variant Also Negotiates (RFC 2295)
// Transparent content negotiation for the request results in a circular reference.
static const inline Status S506_Variant_Also_Negotiates{.code=506, .message=""};

// 507 Insufficient Storage (WebDAV; RFC 4918)
// The server is unable to store the representation needed to complete the request.
static const inline Status S507_Insufficient_Storage{.code=507, .message=""};

// 508 Loop Detected (WebDAV; RFC 5842)
// The server detected an infinite loop while processing the request (sent instead of 208 Already Reported).
static const inline Status S508_Loop_Detected{.code=508, .message=""};

// 509 Bandwidth Limit Exceeded (Apache Web Server/cPanel)
// The server has exceeded the bandwidth specified by the server administrator; this is often used by shared hosting providers to limit the bandwidth of customers.
static const inline Status S509_Bandwidth_Limit_Exceeded{.code=509, .message=""};

// 510 Not Extended (RFC 2774)
// Further extensions to the request are required for the server to fulfil it.
static const inline Status S510_Not_Extended{.code=510, .message=""};

// 511 Network Authentication Required (RFC 6585)
// The client needs to authenticate to gain network access. Intended for use by intercepting proxies used to control access to the network (e.g., "captive portals" used to require agreement to Terms of Service before granting full Internet access via a Wi-Fi hotspot).
static const inline Status S511_Network_Authentication_Required{.code=511, .message=""};

// 520 Web Server Returned an Unknown Error
// Cloudflare extension
// The origin server returned an empty, unknown, or unexplained response to Cloudflare.
static const inline Status S520_Web_Server_Returned_an_Unknown_Error{.code=520, .message=""};

// 521 Web Server Is Down
// Cloudflare extension
// The origin server has refused the connection from Cloudflare.
static const inline Status S521_Web_Server_Is_Down{.code=521, .message=""};

// 522 Connection Timed Out
// Cloudflare extension
// Cloudflare could not negotiate a TCP handshake with the origin server.
static const inline Status S522_Connection_Timed_Out{.code=522, .message=""};

// 523 Origin Is Unreachable
// Cloudflare extension
// Cloudflare could not reach the origin server; for example, if the DNS records for the origin server are incorrect.
static const inline Status S523_Origin_Is_Unreachable{.code=523, .message=""};

// 524 A Timeout Occurred
// Cloudflare extension
// Cloudflare was able to complete a TCP connection to the origin server, but did not receive a timely HTTP response.
static const inline Status S524_A_Timeout_Occurred{.code=524, .message=""};

// 525 SSL Handshake Failed
// Cloudflare extension
// Cloudflare could not negotiate a SSL/TLS handshake with the origin server.
static const inline Status S525_SSL_Handshake_Failed{.code=525, .message=""};

// 526 Invalid SSL Certificate
// Cloudflare extension
// Cloudflare could not validate the SSL certificate on the origin web server.
static const inline Status S526_Invalid_SSL_Certificate{.code=526, .message=""};

// 527 Railgun Error
// Cloudflare extension
// Error 527 indicates an interrupted connection between Cloudflare and the origin server's Railgun server.
static const inline Status S527_Railgun_Error{.code=527, .message=""};

// 529 Site is overloaded
// Used by Qualys in the SSLLabs server testing API to signal that the site can't process the request.
static const inline Status S529_Site_is_overloaded{.code=529, .message=""};

// 598 (Informal convention) Network read timeout error
// Used by some HTTP proxies to signal a network read timeout behind the proxy to a client in front of the proxy.
static const inline Status S598_Network_read_timeout_error{.code=598, .message=""};

}; // struct status
} //namespace k2
