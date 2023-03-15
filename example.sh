#!/bin/sh
go run main.go -dbname feedback -host postgres.examplehost.com -port 5432 -password 'hunter1' -table trade -src transactional -dst shipping -user feedback_user -id trade_id -newcol "BOOLEAN DEFAULT FALSE NOT NULL" -sslmode=1
