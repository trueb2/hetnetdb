#!/bin/bash

AUTH="Authorization: Bearer $1"
echo '{ "text": "select count(*) from hndefault" }' | http post :6969/query/submit "$AUTH"

