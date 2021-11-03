#!/usr/bin/python3

# extracts logs from logki using logcli
import datetime
import json
import sys, getopt
import os
import subprocess

LIMIT=99999
BATCH=5000

def main(argv):
    if len(argv)<=0:
        print("Usage: extract_logs.py -f <from> -t <to> -p <path> -o <org_id>")
        sys.exit(2)
    try:
        opts, args = getopt.getopt(argv, "f:t:p:o:")
    except getopt.error as msg:
        print(msg)
        print("Usage: extract_logs.py -f <from> -t <to> -p <path> -o <org_id>")
        sys.exit(2)

    # validate start and end date, and convert to timezone
    start_date = opts[0][1]
    end_date = opts[1][1]
    path = opts[2][1]
    org = opts[3][1]

    try:
        current = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        start_tz = current.strftime('%Y-%m-%dT00:00:00Z')
        current = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        end_tz = current.strftime('%Y-%m-%dT23:59:59Z')
    except ValueError:
        print("Invalid date")
        sys.exit(2)

    # validate that path exists
    if not os.path.isdir(path):
        print("Invalid path")
        sys.exit(2)

    # now list all the possible labels from logcli
    process = subprocess.check_output("logcli labels --org-id='%s' k8s_namespace_name --output=default" % org, shell=True)
    labels = process.decode("utf-8").split()

    # iterate over all labels    
    for label in labels:
        namespace_start = start_tz
        counter = 0

        # now execute call to retrieve data and store to file
        while True:
            target_file = "%s/%s_%d.jsonl" % (path, label, counter)
            print(target_file)
            print(namespace_start)
            process = subprocess.check_output("logcli query --timezone=UTC --from='%s' --to='%s' --org-id='%s' --output=jsonl --forward "
                "--limit=%d --batch=%d '{k8s_namespace_name=\"%s\"}' > %s" % (namespace_start, end_tz, org, LIMIT, BATCH, label, target_file), shell=True)

            # exit if number of lines in the file is lower than limit
            with open(target_file, "r") as fp:
                x = len(fp.readlines())
            if x < LIMIT:
                print("i reached the end")
                break

            # we need to query the last time of the generated file and start from there
            with open(target_file, "rb") as file:
                file.seek(-2, os.SEEK_END)
                while file.read(1) != b'\n':
                    file.seek(-2, os.SEEK_CUR)

                last_line = json.loads(file.readline().decode())
            file_ts = last_line["timestamp"]

            # we may get different date formats
            if "." not in file_ts:
                file_ts = file_ts + ".0"
            if datetime.datetime.strptime(file_ts, "%Y-%m-%dT%H:%M:%S.%fZ")>=datetime.datetime.strptime(end_tz, "%Y-%m-%dT%H:%M:%SZ"):
                print("i passed the limit")
                break

            # update ts and counter
            namespace_start = file_ts
            counter+=1

if __name__ =="__main__":
    main(sys.argv[1:])