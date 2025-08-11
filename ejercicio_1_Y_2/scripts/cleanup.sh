#!/bin/bash
find resources/staging -type d -name "YYYY=*" -mtime +45 -exec rm -rf {} \;
