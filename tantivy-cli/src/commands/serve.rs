/// This tantivy command starts a http server (by default on port 3000)
///
/// Currently the only entrypoint is /api/
/// and it takes the following query string argument
///
/// - `q=` :    your query
///  - `nhits`:  the number of hits that should be returned. (default to 10)
///
///
/// For instance, the following call should return the 20 most relevant
/// hits for fulmicoton.
///
///     http://localhost:3000/api/?q=fulmicoton&nhits=20
///

/// Ref: https://github.com/quickwit-oss/tantivy/blob/main/examples/snippet.rs

use crate::timer::TimerTree;
use clap::ArgMatches;
use iron::mime::Mime;
use iron::prelude::*;
use iron::status;
use iron::typemap::Key;
use mount::Mount;
use persistent::Write;
use serde_derive::Serialize;
use std::convert::From;
use std::error::Error;
use std::fmt::{self, Debug};
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use tantivy::query::Query;
use tantivy::Searcher;
use tantivy::collector::{TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::NamedFieldDocument;
use tantivy::schema::Schema;
use tantivy::schema::Term;
use tantivy::schema::OwnedValue;
use tantivy::Document;
use tantivy::Index;
use tantivy::{IndexReader, IndexWriter};
use tantivy::TantivyDocument;
use tantivy::TantivyError::InvalidArgument;
use tantivy::snippet::{SnippetGenerator};
use tantivy::snippet::Snippet;
use tantivy::schema::Field;
use tantivy::ReloadPolicy;
use urlencoded::UrlEncodedQuery;
use bodyparser::Json;
use serde_json::Map;
use serde_json::Value;

pub fn run_serve_cli(matches: &ArgMatches) -> Result<(), String> {
    let index_directory = PathBuf::from(matches.get_one::<String>("index").unwrap());
    let port = ArgMatches::get_one(matches, "port").unwrap_or(&3000usize);
    let fallback = "localhost".to_string();
    let host_str = matches.get_one::<String>("host").unwrap_or(&fallback);
    let host = format!("{}:{}", host_str, port);
    run_serve(index_directory, &host).map_err(|e| format!("{:?}", e))
}

#[derive(Serialize)]
struct Serp {
    q: String,
    hits: Vec<Hit>,
    timings: TimerTree,
}

#[derive(Serialize)]
struct Hit {
    doc: NamedFieldDocument,
    snip: String,
}

struct IndexServer {
    reader: IndexReader,
    query_parser: QueryParser,
    schema: Schema,
    writer: IndexWriter
}

impl IndexServer {
    fn load(path: &Path) -> tantivy::Result<IndexServer> {
        let index = Index::open_in_dir(path)?;
        let schema = index.schema();

        // Improve searching and ranking,
        // Don't search in urls unless specified
        let title_field = schema.get_field("title").unwrap();
        let body_field = schema.get_field("body").unwrap();
        let search_fields = vec![title_field, body_field];

        let mut query_parser =
            QueryParser::for_index(&index, search_fields);

        // Do AND for query terms instead of OR
        query_parser.set_conjunction_by_default();

        // Default boost, if not set is 1.0
        // https://github.com/quickwit-oss/tantivy/blob/4aa8cd24707be1255599284f52eb6d388cf86ae8/src/query/query_parser/query_parser.rs#L687
        query_parser.set_field_boost(title_field, 1.5);

        // One may miss the meta tag, after all it is metadata, but body should take higher priority
        query_parser.set_field_boost(body_field, 1.0);

        let reader = index.reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        let writer : IndexWriter = index.writer(50_000_000)?;
        Ok(IndexServer {
            reader,
            query_parser,
            schema,
            writer
        })
    }

    fn create_hit<D: Document>(&self, doc: D, snippet: String) -> Hit {
        let mut named_doc = doc.to_named_doc(&self.schema);
        named_doc.0.remove("body");
        Hit {
            doc: named_doc,
            snip: snippet
        }
    }

    fn search(&self, q: String, num_hits: usize, _offset: usize) -> tantivy::Result<Serp> {
        let query = self
            .query_parser
            .parse_query(&q)
            .expect("Parsing the query failed");
        // TODO: Maybe use parse_query_lenient when parsing fails?

        let searcher = self.reader.searcher();
        let mut timer_tree = TimerTree::default();
        let top_docs = {
            let _search_timer = timer_tree.open("search");
            searcher.search(
                &query,
                &(TopDocs::with_limit(num_hits)),
            )?
        };

        let hits: Vec<Hit> =
            top_docs
                .iter()
                .map(|(_, doc_address)| {
                    let doc = searcher.doc::<TantivyDocument>(*doc_address).unwrap();
                    let body_field = self.schema.get_field("body").unwrap();

                    let snippet : String = 
                        self
                            .gen_html_snippet(&doc, &searcher, &*query, body_field)
                            .to_html()

                    self.create_hit(doc, snippet)
                })
                .collect();
        Ok(Serp {
            q,
            hits,
            timings: timer_tree,
        })
    }

    fn gen_html_snippet(
        &self,
        doc: &TantivyDocument,
        searcher: &Searcher,
        query: &dyn Query,
        field: Field
    ) -> Snippet {
        SnippetGenerator::create(&searcher, &*query, field)
            .unwrap()
            .snippet_from_doc(doc)
    }

    // https://github.com/quickwit-oss/tantivy/blob/main/examples/deleting_updating_documents.rs
    fn delete(&mut self, q: String) -> tantivy::Result<String> {

        let url = self.schema.get_field("url").unwrap();
        let term = Term::from_field_text(url, &q);

        let writer = &mut self.writer;
        // delete_term returns nothing but opstamp which is a number
        let _ = writer.delete_term(term.clone());
        let _ = writer.commit();

        Ok("true".to_string())
    }

    fn validate_json_for_index(&self, json: serde_json::Value) -> Option<tantivy::TantivyError> {
        if json.is_object() {
            let obj : &Map<String, Value> = json.as_object().unwrap();

            for key in ["url", "title", "body"].iter() {
                match obj.get_key_value::<String>(&key.to_string()) {
                    None => return Some(
                        InvalidArgument(format!("json body must contain \"{}\" field.", key))
                    ),
                    Some(val) if !val.1.is_string() => return Some(
                        InvalidArgument(
                            format!("\"{}\" field must have a string value.", key)
                        )
                    ),
                    Some(_) => {},
                }
            }
        } else {
            return Some(
                InvalidArgument("json body must be an object.".to_string())
            )
        }

        return None;
    }

    fn get_length(&self, doc: &TantivyDocument, field_name: &str) -> tantivy::Result<usize> {
        let field = self.schema.get_field(field_name).unwrap();
        return match (*doc).get_first(field).unwrap().into() {
            OwnedValue::Str(string) => Ok(string.len()),
            _ => Err(InvalidArgument("Field value must be of string type.".to_string()))
        };
    }

    fn index_json(&mut self, json: serde_json::Value) -> tantivy::Result<String> {

        // validate json since tantivy will also accept empty docs and we want to ensure all fields
        // are present
        if let Some(err) = self.validate_json_for_index(json.clone()) {
            return Err(err);
        }

        let json_str : &str = &serde_json::to_string(&json).unwrap();
        match TantivyDocument::parse_json(&self.schema, json_str) {
            Ok(doc) => {
                let content_length =
                      self.get_length(&doc, "body").unwrap()
                    + self.get_length(&doc, "title").unwrap();

                let writer = &mut self.writer;
                let _ = writer.add_document(doc);
                let _ = writer.commit();
                Ok(content_length.to_string())
            }
            Err(err) => Err(err.into())
        }
    }

    // for debugging
    #[allow(dead_code)]
    fn print_all_docs(&self) {
        let searcher = self.reader.searcher();
        for segment_reader in searcher.segment_readers() {

            let store = segment_reader.get_store_reader(50_000_000).unwrap();

            for doc in store.iter::<tantivy::TantivyDocument>(segment_reader.alive_bitset()) {
                println!("{}", doc.unwrap().to_json(&self.schema));
            }
        }
    }
}

impl Key for IndexServer {
    type Value = IndexServer;
}

#[derive(Debug)]
struct StringError(String);

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for StringError {
    fn description(&self) -> &str {
        &self.0
    }
}

fn search(req: &mut Request<'_, '_>) -> IronResult<Response> {
    let binding = req.get::<Write<IndexServer>>().unwrap().clone();
    let index_server = binding.lock().unwrap();

    req.get_ref::<UrlEncodedQuery>()
        .map_err(|_| {
            IronError::new(
                StringError(String::from("Failed to decode error")),
                status::BadRequest,
            )
        })
        .and_then(|qs_map| {
            let num_hits: usize = qs_map
                .get("nhits")
                .and_then(|nhits_str| usize::from_str(&nhits_str[0]).ok())
                .unwrap_or(10);
            let query = qs_map.get("q").ok_or_else(|| {
                IronError::new(
                    StringError(String::from("Parameter q is missing from the query")),
                    status::BadRequest,
                )
            })?[0]
                .clone();
            let offset: usize = qs_map
                .get("offset")
                .and_then(|offset_str| usize::from_str(&offset_str[0]).ok())
                .unwrap_or(0);
            let serp = index_server.search(query, num_hits, offset).unwrap();
            let resp_json = serde_json::to_string_pretty(&serp).unwrap();
            let content_type = "application/json".parse::<Mime>().unwrap();
            Ok(Response::with((
                content_type,
                status::Ok,
                resp_json.to_string(),
            )))
        })
}

fn delete(req: &mut Request<'_, '_>) -> IronResult<Response> {
    let binding = req.get::<Write<IndexServer>>().unwrap().clone();
    let mut index_server = binding.lock().unwrap();

    req.get_ref::<UrlEncodedQuery>()
        .map_err(|_| {
            IronError::new(
                StringError(String::from("Failed to decode error")),
                status::BadRequest,
            )
        })
        .and_then(|qs_map| {
            let url = qs_map.get("url").ok_or_else(|| {
                IronError::new(
                    StringError(String::from("Parameter url is missing")),
                    status::BadRequest,
                )
            })?[0]
                .clone();

            index_server.delete(url).unwrap();

            let content_type = "application/json".parse::<Mime>().unwrap();
            Ok(Response::with((
                content_type,
                status::Ok,
                "true".to_string(),
            )))
        })
}

fn index_handler(req: &mut Request<'_, '_>) -> IronResult<Response> {
    let json_body = req.get::<Json>();
    let content_type = "application/json".parse::<Mime>().unwrap();

    match json_body {

        Ok(Some(json_body)) => {

            let binding = req.get::<Write<IndexServer>>().unwrap().clone();
            let mut index_server = binding.lock().unwrap();

            match index_server.index_json(json_body) {
                Ok(msg) => Ok(Response::with((
                    content_type,
                    status::Ok,
                    msg,
                ))),
                Err(err) => Ok(Response::with((
                    content_type,
                    status::BadRequest,
                    err.to_string(),
                )))
            }
        }

        Ok(None) => {
            Ok(Response::with((
                content_type,
                status::BadRequest,
                "No data received. Expecting json body in request payload.".to_string(),
            )))
        }

        Err(_) => {
            Ok(Response::with((
                content_type,
                status::BadRequest,
                "Parsing failed.".to_string(),
            )))
        }
    }
}

fn run_serve(directory: PathBuf, host: &str) -> tantivy::Result<()> {
    let mut mount = Mount::new();
    let server = IndexServer::load(&directory)?;

    mount.mount("/api", search);
    mount.mount("/delete", delete);
    mount.mount("/index", index_handler);

    let mut middleware = Chain::new(mount);
    middleware.link(Write::<IndexServer>::both(server));

    println!("listening on http://{}", host);
    Iron::new(middleware).http(host).unwrap();
    Ok(())
}

// TODO: Add tests
