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

use clap::ArgMatches;
use iron::{mime::Mime, prelude::*, status, typemap::Key};
use mount::Mount;
use persistent::Write;
use serde_derive::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::HashMap,
    convert::From,
    error::Error,
    fmt::{self, Debug},
    path::{Path, PathBuf},
    str::FromStr,
};
use tantivy::{
    collector::TopDocs,
    doc,
    query::{Query, QueryParser},
    schema::{Field, NamedFieldDocument, OwnedValue, Schema, Term},
    snippet::{Snippet, SnippetGenerator},
    Document, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TantivyDocument,
    TantivyError, TantivyError::InvalidArgument,
};
use urlencoded::UrlEncodedQuery;
use bodyparser::Json;

use crate::timer::TimerTree;

// Hardcoded list of technologies and their keywords
fn get_tech_keywords() -> HashMap<&'static str, Vec<&'static str>> {
    let mut map = HashMap::new();
    // AI generated keywords
    map.insert("angular.io", vec!["angular", "components", "spa"]);
    map.insert("api.drupal.org", vec!["drupal", "modules", "themes"]);
    map.insert("api.haxe.org", vec!["haxe", "cross compile", "macro"]);
    map.insert("api.qunitjs.com", vec!["qunit", "assertions", "test fixtures"]);
    map.insert("babeljs.io", vec!["babel", "transpile", "presets"]);
    map.insert("backbonejs.org", vec!["backbone", "events", "routers"]);
    map.insert("bazel.build", vec!["bazel", "hermetic", "reproducible"]);
    map.insert("bluebirdjs.com", vec!["bluebird", "promises", "coroutines"]);
    map.insert("bower.io", vec!["bower", "frontend", "packages"]);
    map.insert("cfdocs.org", vec!["cfml", "tags", "coldfusion"]);
    map.insert("clojure.org", vec!["clojure", "persistent", "transducers"]);
    map.insert("clojuredocs.org", vec!["clojure", "core async", "spec"]);
    map.insert("codecept.io", vec!["codecept", "bdd", "helpers"]);
    map.insert("codeception.com", vec!["codeception", "acceptance", "unit"]);
    map.insert("codeigniter.com", vec!["codeigniter", "mvc", "active record"]);
    map.insert("coffeescript.org", vec!["coffeescript", "indent", "literate"]);
    map.insert("cran.r-project.org", vec!["r", "dataframes", "bioconductor"]);
    map.insert("crystal-lang.org", vec!["crystal", "fibers", "macros"]);
    map.insert("forum.crystal-lang.org", vec!["crystal", "shards", "concurrency"]);
    map.insert("dart.dev", vec!["dart", "isolates", "mixins"]);
    map.insert("dev.mysql.com", vec!["mysql", "innodb", "replication"]);
    map.insert("developer.apple.com", vec!["apple", "swiftui", "metal"]);
    map.insert("developer.mozilla.org", vec!["mdn", "web apis", "wasm"]);
    map.insert("developer.wordpress.org", vec!["wordpress", "hooks", "gutenberg"]);
    map.insert("doc.deno.land", vec!["deno", "permissions", "fresh"]);
    map.insert("doc.rust-lang.org", vec!["rust", "borrowck", "macros"]);
    map.insert("docs.astro.build", vec!["astro", "islands", "mdx"]);
    map.insert("docs.aws.amazon.com", vec!["aws", "lambda", "s3"]);
    map.insert("docs.brew.sh", vec!["brew", "formulae", "casks"]);
    map.insert("docs.chef.io", vec!["chef", "cookbooks", "inspec"]);
    map.insert("docs.cypress.io", vec!["cypress", "fixtures", "mocking"]);
    map.insert("docs.influxdata.com", vec!["influxdb", "flux", "bucket"]);
    map.insert("docs.julialang.org", vec!["julia", "multiple dispatch", "jupyter"]);
    map.insert("docs.microsoft.com", vec!["microsoft", "dotnet", "azure"]);
    map.insert("docs.npmjs.com", vec!["npm", "registry", "workspaces"]);
    map.insert("docs.oracle.com", vec!["oracle", "plsql", "jdbc"]);
    map.insert("docs.phalconphp.com", vec!["phalcon", "orm", "volt"]);
    map.insert("docs.python.org", vec!["python", "decorators", "async"]);
    map.insert("docs.rs", vec!["rust", "cargo", "no std"]);
    map.insert("docs.ruby-lang.org", vec!["ruby", "gems", "blocks"]);
    map.insert("docs.saltproject.io", vec!["salt", "states", "pillar"]);
    map.insert("docs.wagtail.org", vec!["wagtail", "streamfield", "snippets"]);
    map.insert("doctrine-project.org", vec!["doctrine", "dql", "migrations"]);
    map.insert("docwiki.embarcadero.com", vec!["embarcadero", "vcl", "fmx"]);
    map.insert("eigen.tuxfamily.org", vec!["eigen", "matrix", "numerical"]);
    map.insert("elixir-lang.org", vec!["elixir", "otp", "ecto"]);
    map.insert("elm-lang.org", vec!["elm", "elm architecture", "ports"]);
    map.insert("en.cppreference.com", vec!["cpp", "stl", "concepts"]);
    map.insert("enzymejs.github.io", vec!["enzyme", "shallow", "adapters"]);
    map.insert("erights.org", vec!["e", "vat", "promises"]);
    map.insert("erlang.org", vec!["erlang", "beam", "supervisors"]);
    map.insert("esbuild.github.io", vec!["esbuild", "bundler", "tree shaking"]);
    map.insert("eslint.org", vec!["eslint", "lint rules", "configs"]);
    map.insert("expressjs.com", vec!["express", "middleware", "routing"]);
    map.insert("fastapi.tiangolo.com", vec!["fastapi", "pydantic", "openapi"]);
    map.insert("flow.org", vec!["flow", "type annotations", "linter"]);
    map.insert("fortran90.org", vec!["fortran", "array", "mpi"]);
    map.insert("fsharp.org", vec!["fsharp", "type providers", "async"]);
    map.insert("getbootstrap.com", vec!["bootstrap", "grid", "responsive"]);
    map.insert("getcomposer.org", vec!["composer", "autoload", "psr"]);
    map.insert("git-scm.com", vec!["git", "rebase", "submodules"]);
    map.insert("gnu.org", vec!["gnu", "gpl", "coreutils"]);
    map.insert("gnucobol.sourceforge.io", vec!["cobol", "mainframe", "legacy"]);
    map.insert("go.dev", vec!["go", "goroutines", "garbage"]);
    map.insert("golang.org", vec!["go", "channels", "interfaces"]);
    map.insert("graphite.readthedocs.io", vec!["graphite", "carbon", "whisper"]);
    map.insert("groovy-lang.org", vec!["groovy", "grails", "spock"]);
    map.insert("gruntjs.com", vec!["grunt", "plugins", "tasks"]);
    map.insert("handlebarsjs.com", vec!["handlebars", "partials", "helpers"]);
    map.insert("haskell.org", vec!["haskell", "monads", "ghc"]);
    map.insert("hex.pm", vec!["hex", "mix", "exunit"]);
    map.insert("hexdocs.pm", vec!["hex", "ex doc", "docs"]);
    map.insert("httpd.apache.org", vec!["apache", "mod ssl", "htaccess"]);
    map.insert("i3wm.org", vec!["i3", "tiling", "workspaces"]);
    map.insert("jasmine.github.io", vec!["jasmine", "matchers", "spies"]);
    map.insert("javascript.info", vec!["javascript", "event loop", "prototype"]);
    map.insert("jekyllrb.com", vec!["jekyll", "liquid", "front matter"]);
    map.insert("jsdoc.app", vec!["jsdoc", "typedef", "tags"]);
    map.insert("julialang.org", vec!["julia", "julia vscode", "revise"]);
    map.insert("knockoutjs.com", vec!["knockout", "observables", "bindings"]);
    map.insert("kotlinlang.org", vec!["kotlin", "coroutines", "dsls"]);
    map.insert("laravel.com", vec!["laravel", "eloquent", "artisan"]);
    map.insert("latexref.xyz", vec!["latex", "bibtex", "packages"]);
    map.insert("learn.microsoft.com", vec!["microsoft", "powershell", "azure"]);
    map.insert("lesscss.org", vec!["less", "mixins", "variables"]);
    map.insert("love2d.org", vec!["love2d", "canvas", "physics"]);
    map.insert("lua.org", vec!["lua", "metatables", "coroutine"]);
    map.insert("man7.org", vec!["linux", "syscalls", "proc"]);
    map.insert("mariadb.com", vec!["mariadb", "galera", "columns"]);
    map.insert("mochajs.org", vec!["mocha", "bdd", "reporters"]);
    map.insert("modernizr.com", vec!["modernizr", "polyfills", "feature"]);
    map.insert("momentjs.com", vec!["moment", "timezone", "duration"]);
    map.insert("mongoosejs.com", vec!["mongoose", "schemas", "populate"]);
    map.insert("next.router.vuejs.org", vec!["vue", "navigation guards", "dynamic"]);
    map.insert("next.vuex.vuejs.org", vec!["vuex", "mutations", "modules"]);
    map.insert("nginx.org", vec!["nginx", "reverse proxy", "load"]);
    map.insert("nim-lang.org", vec!["nim", "metaprogramming", "gc"]);
    map.insert("nixos.org", vec!["nixos", "derivations", "channels"]);
    map.insert("nodejs.org", vec!["nodejs", "npm", "event loop"]);
    map.insert("npmjs.com", vec!["npm", "dependencies", "semver"]);
    map.insert("ocaml.org", vec!["ocaml", "opam", "functors"]);
    map.insert("odin-lang.org", vec!["odin", "procedural", "llvm"]);
    map.insert("openjdk.java.net", vec!["openjdk", "hotspot", "jmh"]);
    map.insert("opentsdb.net", vec!["opentsdb", "metrics", "put"]);
    map.insert("perldoc.perl.org", vec!["perl", "cpan", "regex"]);
    map.insert("php.net", vec!["php", "composer", "namespaces"]);
    map.insert("playwright.dev", vec!["playwright", "browsers", "fixtures"]);
    map.insert("pointclouds.org", vec!["pcl", "point cloud", "registration"]);
    map.insert("postgresql.org", vec!["postgresql", "postgis", "jsonb"]);
    map.insert("prettier.io", vec!["prettier", "format", "config"]);
    map.insert("pugjs.org", vec!["pug", "mixins", "filters"]);
    map.insert("pydata.org", vec!["pydata", "numpy", "pandas"]);
    map.insert("pytorch.org", vec!["pytorch", "tensors", "autograd"]);
    map.insert("qt.io", vec!["qt", "qml", "signals"]);
    map.insert("r-project.org", vec!["r", "tidyverse", "ggplot"]);
    map.insert("react-bootstrap.github.io", vec!["react", "components", "hooks"]);
    map.insert("reactivex.io", vec!["rxjs", "observables", "operators"]);
    map.insert("reactjs.org", vec!["react", "jsx", "context"]);
    map.insert("reactnative.dev", vec!["react", "native", "bridge"]);
    map.insert("reactrouterdotcom.fly.dev", vec!["react", "history", "link"]);
    map.insert("readthedocs.io", vec!["readthedocs", "sphinx", "rtd"]);
    map.insert("readthedocs.org", vec!["readthedocs", "versions", "search"]);
    map.insert("redis.io", vec!["redis", "pubsub", "lua"]);
    map.insert("redux.js.org", vec!["redux", "reducers", "middleware"]);
    map.insert("requirejs.org", vec!["requirejs", "amd", "optimizer"]);
    map.insert("rethinkdb.com", vec!["rethinkdb", "changefeeds", "joins"]);
    map.insert("ruby-doc.org", vec!["ruby", "yard", "ri"]);
    map.insert("ruby-lang.org", vec!["ruby", "rake", "erb"]);
    map.insert("rust-lang.org", vec!["rust", "ownership", "traits"]);
    map.insert("rxjs.dev", vec!["rxjs", "subjects", "schedulers"]);
    map.insert("sass-lang.com", vec!["sass", "scss", "mixins"]);
    map.insert("scala-lang.org", vec!["scala", "akka", "cats"]);
    map.insert("scikit-image.org", vec!["scikit", "image", "ndimage"]);
    map.insert("scikit-learn.org", vec!["scikit", "svm", "pipeline"]);
    map.insert("spring.io", vec!["spring", "boot", "dependency"]);
    map.insert("sqlite.org", vec!["sqlite", "sql", "fts"]);
    map.insert("stdlib.ponylang.io", vec!["pony", "capabilities", "actors"]);
    map.insert("superuser.com", vec!["superuser", "answers", "moderators"]);
    map.insert("svelte.dev", vec!["svelte", "stores", "actions"]);
    map.insert("swift.org", vec!["swift", "swiftui", "codable"]);
    map.insert("tailwindcss.com", vec!["tailwind", "utility", "purge"]);
    map.insert("twig.symfony.com", vec!["twig", "filters", "inheritance"]);
    map.insert("typescriptlang.org", vec!["typescript", "types", "generics"]);
    map.insert("underscorejs.org", vec!["underscore", "collections", "chaining"]);
    map.insert("vitejs.dev", vec!["vite", "hmr", "plugins"]);
    map.insert("vitest.dev", vec!["vitest", "coverage", "snapshot"]);
    map.insert("vuejs.org", vec!["vue", "composition", "directives"]);
    map.insert("vueuse.org", vec!["vueuse", "composables", "sensors"]);
    map.insert("webpack.js.org", vec!["webpack", "loaders", "chunks"]);
    map.insert("wiki.archlinux.org", vec!["arch", "pacman", "aur"]);
    map.insert("www.chaijs.com", vec!["chai", "assert", "should"]);
    map.insert("www.electronjs.org", vec!["electron", "ipc", "asar"]);
    map.insert("www.gnu.org", vec!["gnu", "gcc", "bash"]);
    map.insert("www.hammerspoon.org", vec!["hammerspoon", "lua", "automation"]);
    map.insert("www.khronos.org", vec!["khronos", "vulkan", "spirv"]);
    map.insert("www.lua.org", vec!["lua", "luajit", "metatable"]);
    map.insert("www.php.net/manual/en/", vec!["php", "phpdoc", "extensions"]);
    map.insert("www.pygame.org", vec!["pygame", "sprite", "surface"]);
    map.insert("www.rubydoc.info", vec!["ruby", "yardoc", "rdoc"]);
    map.insert("www.statsmodels.org", vec!["statsmodels", "regression", "timeseries"]);
    map.insert("www.tcl.tk", vec!["tcl", "tk", "expect"]);
    map.insert("www.terraform.io", vec!["terraform", "providers", "state"]);
    map.insert("www.vagrantup.com", vec!["vagrant", "boxes", "provisioners"]);
    map.insert("www.yiiframework.com", vec!["yii", "gii", "activerecord"]);
    map.insert("yarnpkg.com", vec!["yarn", "berry", "workspaces"]);
    map
}

// Search warmer
struct SearchWarmer {
    tech_keywords: HashMap<&'static str, Vec<&'static str>>,
}

impl SearchWarmer {
    fn new() -> Self {
        SearchWarmer {
            tech_keywords: get_tech_keywords(),
        }
    }

    fn warm(&self, query_parser: &QueryParser, searcher: &Searcher) -> Result<(), TantivyError> {

        for (_site, keywords) in self.tech_keywords.iter() {

            if !keywords.is_empty() {

                let query = query_parser.parse_query(&format!("{}", keywords[0]))?;
                let _ = searcher.search(
                    &query,
                    &tantivy::collector::TopDocs::with_limit(2)
                )?;

                let query2 = query_parser.parse_query(&format!("{} {}", keywords[0], keywords[1]))?;
                let _ = searcher.search(
                    &query2,
                    &tantivy::collector::TopDocs::with_limit(2)
                );

                let _ = searcher.search(
                    &query_parser.parse_query(&format!("{} {}", keywords[0], keywords[2]))?,
                    &tantivy::collector::TopDocs::with_limit(2)
                );
            }
        }
        Ok(())
    }
}

pub fn run_serve_cli(matches: &ArgMatches) -> Result<(), String> {
    let index_directory = PathBuf::from(matches.get_one::<String>("index").unwrap());
    let port = ArgMatches::get_one(matches, "port").unwrap_or(&3000usize);
    let fallback = "localhost".to_string();
    let host_str = matches.get_one::<String>("host").unwrap_or(&fallback);
    let host = format!("{}:{}", host_str, port);
    run_serve(index_directory, &host).map_err(|e| format!("{:?}", e))
}

// quotifying each term allows you
// derived after observing the query AST
fn escape_tantivy_query(input: &str) -> String {
    input.split_whitespace() // Split into terms by whitespace
        .map(|term| {

            // Escape special characters in the term
            let mut escaped = String::new();
            for c in term.chars() {
                match c {
                    '\\' | '"' | '\'' => {
                        println!("{}", c);
                        escaped.push('\\');
                        escaped.push(c);
                        println!("{}", escaped)
                    }
                    _ => {
                        escaped.push(c);
                    },
                }
            }

            escaped
        })
        .map(|esc| {
            let mut f = String::new();
            f.push_str("\"");
            f.push_str(&esc);
            f.push_str("\"");
            f
        })
        .collect::<Vec<_>>()
        .join(" ") // Rejoin terms with spaces
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

        let warmer = SearchWarmer::new();
        warmer.warm(&query_parser, &reader.searcher())?;

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
        let (query, _err) = self
            .query_parser
            .parse_query_lenient(&escape_tantivy_query(&q));

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
                            .to_html();

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

// AI + dev
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reserved_keywords() {
        // Exact reserved keywords should be quoted
        assert_eq!(escape_tantivy_query("AND"), "\"AND\"");
        assert_eq!(escape_tantivy_query("OR"), "\"OR\"");
        assert_eq!(escape_tantivy_query("IN"), "\"IN\"");
    }

    #[test]
    fn test_partial_keywords() {
        // Substrings/partial matches should not be quoted
        assert_eq!(escape_tantivy_query("ANDROID"), "\"ANDROID\"");
        assert_eq!(escape_tantivy_query("ORACLE"), "\"ORACLE\"");
        assert_eq!(escape_tantivy_query("INPUT"), "\"INPUT\"");
    }

    fn test_case_sensitivity() {
        // Case-sensitive matching (only uppercase is reserved)
        assert_eq!(escape_tantivy_query("and"), "and");
        assert_eq!(escape_tantivy_query("or"), "or");
        assert_eq!(escape_tantivy_query("in"), "in");
    }

    #[test]
    fn test_keywords_with_special_chars() {
        // Keywords with special characters should be escaped and quoted
        assert_eq!(escape_tantivy_query("+AND"), "\"+AND\"");
        assert_eq!(escape_tantivy_query("OR~"), "\"OR~\"");
        assert_eq!(escape_tantivy_query("IN*"), "\"IN*\"");
    }

    #[test]
    fn test_mixed_keyword_usage() {
        // Mixed keyword-like terms
        assert_eq!(escape_tantivy_query("AND=OR"), "\"AND=OR\"");
        assert_eq!(escape_tantivy_query("IN/OUT"), "\"IN/OUT\"");
    }

    #[test]
    fn test_mixed_keyword_usage_1() {
        // Mixed keyword-like terms
        assert_eq!(escape_tantivy_query("a AND b"), "\"a\" \"AND\" \"b\"");
    }

    #[test]
    fn test_quoted_keywords() {
        assert_eq!(escape_tantivy_query("a \"AND\" b"), "\"a\" \"\\\"AND\\\"\" \"b\"");

        // user says: a \"AND\" b
        // pass to tantivy: "a" "\\\"AND\\\"" "b"
        assert_eq!(escape_tantivy_query("a \\\"AND\\\" b"), "\"a\" \"\\\\\\\"AND\\\\\\\"\" \"b\"");

        assert_eq!(escape_tantivy_query("'OR'"), "\"\\\'OR\\\'\"");
    }

    #[test]
    fn test_field_specific_queries() {
        // Colon-containing terms should be escaped
        assert_eq!(
            escape_tantivy_query("field:AND"),
            "\"field:AND\""
        );
        assert_eq!(
            escape_tantivy_query("tags:IN"),
            "\"tags:IN\""
        );
    }

    #[test]
    fn test_boolean_combinations() {
        // Boolean-like combinations in terms
        assert_eq!(
            escape_tantivy_query("ANDOR"),
            "\"ANDOR\""
        );
        assert_eq!(
            escape_tantivy_query("NOTIN"),
            "\"NOTIN\""
        );
    }
}
