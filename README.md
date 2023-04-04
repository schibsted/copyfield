# Copyfield

### Description

Copy a value from one field in a table, to another field in the same table, in a PostgreSQL database.

The entire process can be stopped with `ctrl-c` and resumed by running the command again, since the progress is stored in `progress.txt`.

### Installation

Requires Go >= 1.17:

    go install github.com/schibsted/copyfield@latest

### Example use

NOTE: Replace `PASSWORD` with your password, for all commands below.

Copy values from the "transactional" field to the "shipping" field:

    copyfield -dbname feedback -host 127.0.0.1 -port 5433 -password PASSWORD -table trade -src transactional -dst shipping -user feedback_user -id trade_id

Copy values from the "transactional" field to the "shipping" field, and create the "shipping" column first if it does not exist:

    copyfield -dbname feedback -host 127.0.0.1 -port 5433 -password PASSWORD -table trade -src transactional -dst shipping -user feedback_user -id trade_id -newcol "BOOLEAN DEFAULT FALSE NOT NULL"

Examine values:

    psql -h localhost -p 5433 --username=feedback_user feedback

For servers that are not running on localhost, adding `-sslmode=1` is most likely needed.

### Reset the counter and remove a column

Remove the file with the overview of the current progress:

    rm progress.txt

Also drop the column, if needed (replace `<table>` and `<col>` with your own):

    ALTER TABLE <table> DROP COLUMN <col>;

### Progress

The progress is stored as a list of table indices in `index.txt`. The file is written to (flushed) every time a row in the database has been modified.

### General info

* Author: Alexander F. Rødseth
* License: [Apache 2](LICENSE)
* Version: 1.0.1

## The contents of the NOTICE file

(Required, per company policy).

```
################################################################################
#                                                                              #
# Copyfield                                                                    #
#                                                                              #
# Copyright 2023 Schibsted                                                     #
#                                                                              #
# Unless required by applicable law or agreed to in writing, software          #
# distributed under the License is distributed on an "AS IS" BASIS,            #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     #
#                                                                              #
# See the License for the specific language governing permissions and          #
# limitations under the License.                                               #
#                                                                              #
################################################################################
```
