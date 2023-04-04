#!/bin/sh
# Copyright 2023 Schibsted. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

go run main.go -dbname feedback -host postgres.examplehost.com -port 5432 -password 'hunter1' -table trade -src transactional -dst shipping -user feedback_user -id trade_id -newcol "BOOLEAN DEFAULT FALSE NOT NULL" -sslmode=1
