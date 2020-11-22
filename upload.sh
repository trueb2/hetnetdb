#!/bin/bash

AUTH="Authorization: Bearer $2"
http --multipart post :6969/tables/upload/1 "$AUTH" csv@$1

