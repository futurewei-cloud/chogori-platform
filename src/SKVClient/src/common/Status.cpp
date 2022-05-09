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

#include "Status.h"
namespace k2 {

bool Status::operator==(const Status& o) { return code == o.code; }

bool Status::operator!=(const Status& o) { return !(code == o.code); }

Status Status::operator()(String message) const {
    return Status{this->code, message};
}

bool Status::is1xxInProgress() const { return code >= 100 && code <= 199; }

bool Status::is2xxOK() const { return code >= 200 && code <= 299; }

bool Status::is3xxActionNeeded() const { return code >= 300 && code <= 399; }

bool Status::is4xxNonRetryable() const { return code >= 400 && code <= 499; }

bool Status::is5xxRetryable() const { return code >= 500 && code <= 599; }

String Status::getDescription() const {
    switch (code) {
        case 100:
            return "Continue";
        case 101:
            return "Switching Protocols";
        case 102:
            return "Processing";
        case 103:
            return "Early Hints";

        case 200:
            return "OK";
        case 201:
            return "Created";
        case 202:
            return "Accepted";
        case 203:
            return "Non-Authoritative Information";
        case 204:
            return "No Content";
        case 205:
            return "Reset Content";
        case 206:
            return "Partial Content";
        case 207:
            return "Multi-Status";
        case 208:
            return "Already Reported";
        case 218:
            return "This is fine";
        case 226:
            return "IM Used";

        case 300:
            return "Multiple Choices";
        case 301:
            return "Moved Permanently";
        case 302:
            return "Found";
        case 303:
            return "See Other";
        case 304:
            return "Not Modified";
        case 305:
            return "Use Proxy";
        case 306:
            return "Switch Proxy";
        case 307:
            return "Temporary Redirect";
        case 308:
            return "Permanent Redirect";

        case 400:
            return "Bad Request";
        case 401:
            return "Unauthorized";
        case 402:
            return "Payment Required";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 405:
            return "Method Not Allowed";
        case 406:
            return "Not Acceptable";
        case 407:
            return "Proxy Authentication Required";
        case 408:
            return "Request Timeout";
        case 409:
            return "Conflict";
        case 410:
            return "Gone";
        case 411:
            return "Length Required";
        case 412:
            return "Precondition Failed";
        case 413:
            return "Payload Too Large";
        case 414:
            return "URI Too Long";
        case 415:
            return "Unsupported Media Type";
        case 416:
            return "Range Not Satisfiable";
        case 417:
            return "Expectation Failed";
        case 418:
            return "I'm a teapot";
        case 419:
            return "Page Expired";
        case 420:
            return "Enhance Your Calm";
        case 421:
            return "Misdirected Request";
        case 422:
            return "Unprocessable Entity";
        case 423:
            return "Locked";
        case 424:
            return "Failed Dependency";
        case 425:
            return "Too Early";
        case 426:
            return "Upgrade Required";
        case 428:
            return "Precondition Required";
        case 429:
            return "Too Many Requests";
        case 440:
            return "Login Time-out";
        case 444:
            return "No Response";
        case 449:
            return "Retry With";
        case 450:
            return "Blocked by Windows Parental Controls";
        case 451:
            return "Unavailable For Legal Reasons";
        case 460:
            return "LB Client Connection Closed";
        case 463:
            return "LB Request Too Large";
        case 494:
            return "Request header too large";
        case 495:
            return "SSL Certificate Error";
        case 496:
            return "SSL Certificate Required";
        case 497:
            return "HTTP Request Sent to HTTPS Port";
        case 499:
            return "Client Closed Request";

        case 500:
            return "Internal Server Error";
        case 501:
            return "Not Implemented";
        case 502:
            return "Bad Gateway";
        case 503:
            return "Service Unavailable";
        case 504:
            return "Gateway Timeout";
        case 505:
            return "HTTP Version Not Supported";
        case 506:
            return "Variant Also Negotiates";
        case 507:
            return "Insufficient Storage";
        case 508:
            return "Loop Detected";
        case 509:
            return "Bandwidth Limit Exceeded";
        case 510:
            return "Not Extended";
        case 511:
            return "Network Authentication Required";
        case 520:
            return "Web Server Returned an Unknown Error";
        case 521:
            return "Web Server Is Down";
        case 522:
            return "Connection Timed Out";
        case 523:
            return "Origin Is Unreachable";
        case 524:
            return "A Timeout Occurred";
        case 525:
            return "SSL Handshake Failed";
        case 526:
            return "Invalid SSL Certificate";
        case 527:
            return "Railgun Error";
        case 529:
            return "Site is overloaded";
        case 598:
            return "Network read timeout error";

        default:
            return "Unknown";
    }
}

} // namespace k2
