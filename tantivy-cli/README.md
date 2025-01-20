[![Docs](https://docs.rs/tantivy/badge.svg)](https://docs.rs/crate/tantivy-cli/)
[![Join the chat at https://discord.gg/MT27AG5EVE](https://shields.io/discord/908281611840282624?label=chat%20on%20discord)](https://discord.gg/MT27AG5EVE)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Crates.io](https://img.shields.io/crates/v/tantivy.svg)](https://crates.io/crates/tantivy-cli)


# tantivy-cli

`tantivy-cli` is the the command line interface for the [tantivy](https://github.com/quickwit-inc/tantivy) search engine. It provides indexing and search capabilities, and is suitable for smaller projects.

## Installing the tantivy CLI.

There are a couple ways to install `tantivy-cli`.

If you are a Rust programmer, you probably have `cargo` installed and you can just from `tantivy-cli` directory:

```bash
cargo install tantivy-cli --locked --path ./
```

## Creating the index:  `new`
 
Let's create a directory in which your index will be stored.

```bash
mkdir prosearch-index
```

We will now initialize the index and create its schema.
The [schema](https://quickwit-oss.github.io/tantivy/tantivy/schema/index.html) defines
the list of your fields, and for each field:
- its name 
- its type, currently `u64`, `i64` or `str`
- how it should be indexed.

You can find more information about the latter on 
[tantivy's schema documentation page](https://quickwit-oss.github.io/tantivy/tantivy/schema/index.html)

In our case, our documents will contain
* a title
* a body 
* a url

We want the title and the body to be tokenized and indexed. We also want 
to add the term frequency and term positions to our index.

Running `tantivy new` will start a wizard that will help you
define the schema of the new index.

Like all the other commands of `tantivy`, you will have to 
pass it your index directory via the `-i` or `--index`
parameter as follows:

```bash
tantivy new -i prosearch-index
```

Answer the questions as follows:

```none

    Creating new index 
    Let's define its schema! 



    New field name  ? title
    Choose Field Type (Text/u64/i64/f64/Date/Facet/Bytes) ? Text
    Should the field be stored (Y/N) ? Y
    Should the field be indexed (Y/N) ? Y
    Should the term be tokenized? (Y/N) ? Y
    Should the term frequencies (per doc) be in the index (Y/N) ? Y
    Should the term positions (per doc) be in the index (Y/N) ? Y
    Add another field (Y/N) ? Y
    
    
    
    New field name  ? body
    Choose Field Type (Text/u64/i64/f64/Date/Facet/Bytes) ? Text
    Should the field be stored (Y/N) ? Y
    Should the field be indexed (Y/N) ? Y
    Should the term be tokenized? (Y/N) ? Y
    Should the term frequencies (per doc) be in the index (Y/N) ? Y
    Should the term positions (per doc) be in the index (Y/N) ? Y
    Add another field (Y/N) ? Y
    
    
    
    New field name  ? url
    Choose Field Type (Text/u64/i64/f64/Date/Facet/Bytes) ? Text
    Should the field be stored (Y/N) ? Y
    Should the field be indexed (Y/N) ? N
    Add another field (Y/N) ? N

```

After the wizard has finished, a `meta.json` should exist in `prosearch-index/meta.json`.
It is a fairly human readable JSON, so you can check its content.

# Indexing the document: `index`


Tantivy's `index` command offers a way to index a json file.
The file must contain one JSON object per line.
The structure of this JSON object must match that of our schema definition.

```json
{"body": "some text", "title": "some title", "url": "http://somedomain.com"}
```

The `index` command will index your document.
By default it will use as 3 thread, each with a buffer size of 1GB split a
across these threads. 


```bash
tantivy index -i ./prosearch-index <your-json-doc>
```

You can change the number of threads by passing it the `-t` parameter, and the total
buffer size used by the threads heap by using the `-m`. Note that tantivy's memory usage
is greater than just this buffer size parameter.

While tantivy is indexing, you can peek at the index directory to check what is happening.

```bash
ls ./prosearch-index
```

The main file is `meta.json`.

You should also see a lot of files with a UUID as filename, and different extensions.
Our index is in fact divided in segments. Each segment acts as an individual smaller index.
Its name is simply a uuid. 

Having too many segments can hurt search performance, so tantivy actually automatically starts
merging segments. 

# Serve the search index: `serve`

Tantivy's cli also embeds a search server.
**NOTE**: This version of tantivy-cli is modified to emit snippets of documents.
You can run it with the following command.

```bash
tantivy serve -i prosearch-index
```

By default, it will serve on port `3000`.

You can search for the top 20 most relevant documents for the query `Barack Obama` by accessing
the following [url](http://localhost:3000/api/?q=barack+obama&nhits=20) in your browser

    http://localhost:3000/api/?q=barack+obama&nhits=20

By default this query is treated as `barack OR obama`.
You can also search for documents that contains both term, by adding a `+` sign before the terms in your query.

    http://localhost:3000/api/?q=%2Bbarack%20%2Bobama&nhits=20
    
Also, `-` makes it possible to remove documents the documents containing a specific term.

    http://localhost:3000/api/?q=-barack%20%2Bobama&nhits=20
    
Finally tantivy handle phrase queries.

    http://localhost:3000/api/?q=%22barack%20obama%22&nhits=20

# Benchmark the index: `bench`

Tantivy's cli provides a simple benchmark tool.
You can run it with the following command.

```bash
tantivy bench -i prosearch-index -n 10 -q queries.txt
```
