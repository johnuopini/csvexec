## Csv Exec
This tool will parse a CSV file then, for every single line, it will create a shell script based on django template
syntax and execute the task in parallel. Failures will be logged.

### Example
You can use the data in the example folder to test it out:
```bash
$ csvexec examples/echo/echo.csv examples/echo/echo.sh.j2
Starting runner 05/03 04:00:38PM '22 +0200:
 - Jobs: 8, total 1034 lines, stop at 1034
 - Csv: examples/echo/echo.csv
 - Template: examples/echo/echo.sh.j2
 - Workdir: /tmp/csvexec
 - Eta: 1m3s [+===>-------------------------] 7.4% [failed:10] [avg:66ms]
```
