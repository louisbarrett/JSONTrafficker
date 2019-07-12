        .--------------.
        |~            ~|
        |H____JSON____H|
        |.------------.|
        ||::..     __ ||
        |'--------'--''|
        | '. ______ .' |
        | _ |======| _ |
        |(_)|======|(_)|
        |___|======|___|
        [______________]
        |##|        |##|
        '""'        '""'
```
    Usage: JSONTrafficker --input=<source> --kinesis-stream=<stream name>

    Options:
    --input=           -i  Input mode <s3,file,stdin>
                           s3://BucketName/Key
                           /tmp/logs.json
                           stdin|-

   --kinesis-stream=   -k  Target kinesis stream name
   --region=           -r  AWS region <us-west-2>
   --help=             -h  Display this help and exit
   Examples:

   JSONTrafficker -i=s3://MyLogsBucket/logdata.json -k security-logs

   JSONTrafficker -i=/tmp/logdata.json -k security-logs

   cat /tmp/logdata | JSONTrafficker -i=stdin
```
