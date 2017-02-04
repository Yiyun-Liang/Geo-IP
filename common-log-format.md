```
10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET /assets/js/lowpro.js HTTP/1.1" 200 10469
```

```
%h %l %u %t \"%r\" %>s %b
```

Where:

%h is the IP address of the client
%l is identity of the client, or "-" if it's unavailable
%u is username of the client, or "-" if it's unavailable
%t is the time that the server finished processing the request. The format is [day/month/year:hour:minute:second zone]
%r is the request line from the client is given (in double quotes). It contains the method, path, query-string, and protocol or the request.
%>s is the status code that the server sends back to the client. You will see see mostly status codes 200 (OK - The request has succeeded), 304 (Not Modified) and 404 (Not Found). See more information on status codes in W3C.org
%b is the size of the object returned to the client, in bytes. It will be "-" in case of status code 304.


A = FOREACH LOG GENERATE ToDate(timestamp, 'dd/MMM/yyyy:HH:mm:ss Z')
  as date, utils.get_country(ip) as country, utils.get_city(ip) as city,
  utils.get_geo(ip) as location, ip, url, (int)status, (int)bytes;
