# HTML Link Extractor

## Features

- **Concurrent & Ordered**: Fetches URLs across worker threads while guaranteeing that output rows preserve the exact order of the input.
- **Zero Dependencies**: Built entirely using Python's standard library (`threading`, `queue`, `urllib`, `html.parser`).
- **Flexible Ingestion**: Accepts inputs via newline-delimited text files (`-f`) or piped standard input (`stdin`).
- **CSS Selector Filtering**: Scopes link extraction using CSS selector syntax (e.g., `#main`, `.products`, `div.products a`).
- **Resilient & Descriptive**: Handles HTTP errors (4xx/5xx), connection drops, and malformed markup without halting execution.

---

## Requirements

- **Python 3.8+**

---

## Usage

### CLI Options

| Flag | Long Flag | Default | Description |
|---|---|---|---|
| `-f` | `--file` | `None` | Path to input file containing one URL per line |
| `-t` | `--threads` | `4` | Number of concurrent fetch worker threads |
| `-s` | `--selector` | `None` | CSS selector to extract only nested links |

---

### Examples

#### 1. File Input

Process URLs from a file and write results directly to `output.csv`:

```bash
python src/main.py -f input.txt > output.csv
```

Increase concurrency for large link batches:

```bash
python src/main.py -f input.txt -t 16 > output.csv
```

#### 2. Standard Input (`stdin`)

Pipe URLs from files, shell scripts, or commands:

```bash
cat input.txt | python src/main.py
```

Process a single URL inline:

```bash
echo "https://web-scraping.dev/products" | python src/main.py
```

#### 3. CSS Selector Filtering (`-s`)

Extract only links nested inside specific DOM containers:

```bash
# Filter by class name
python src/main.py -f input.txt -s ".products" > output.csv

# Filter by tag and class
python src/main.py -f input.txt -s "div.products" > output.csv

# Filter by descendant hierarchy
echo "https://web-scraping.dev/products" | python src/main.py -s "nav .dropdown-menu"
```

---

## Output Specification

Output is streamed as CSV to `stdout`.

### CSV Columns

| Column | Type | Description |
|---|---|---|
| `input_link` | `string` | The source URL requested |
| `links` | `json_array` | Deduplicated list of resolved absolute URLs matching the criteria |
| `status` | `int` | `1` on success (HTTP 2xx + parsed), `0` on error |
| `message` | `string` | `"OK"` on success, or the failure reason (e.g., `HTTP Error 404: Not Found`) |

---

## Testing

Run the full automated test suite (unit tests, selector tests, and end-to-end integration tests):

```bash
# Run all discovered tests
python -m unittest discover -s tests -p "test_*.py" -v

# Run selector-specific tests
python tests/test_selector.py
```