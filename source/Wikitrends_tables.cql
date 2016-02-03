CREATE TABLE wikistats.hrpageviews ( event_time timestamp, title text, pageviews int, PRIMARY KEY ((event_time), title);
CREATE TABLE wikistats.dapageviews ( event_time timestamp, title text, pageviews int, PRIMARY KEY ((event_time), title);
CREATE TABLE wikistats.top100trending ( event_time timestamp, pageviews int, title text, PRIMARY KEY ((event_time), title));
CREATE TABLE wikistats.top100rising ( event_time timestamp, pageviews int, title text, PRIMARY KEY ((event_time), title));

CREATE TABLE wikistats.top100trendinghist ( refhour timestamp, title text, event_time timestamp, pageviews int, PRIMARY KEY ((refhour, title), event_time )) WITH CLUSTERING ORDER BY (event_time ASC);
CREATE TABLE wikistats.top100risinghist ( refhour timestamp, title text, event_time timestamp, pageviews int,  PRIMARY KEY ((refhour, title), event_time)) WITH CLUSTERING ORDER BY (event_time ASC);