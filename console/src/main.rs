mod app;
mod discovery;
mod ui;

use app::App;
use crossterm::event::{self, Event};
use ratatui::DefaultTerminal;
use std::time::Duration;
use structopt::StructOpt;
use url::Url;

/// Default port for the observability gRPC service.
pub const DEFAULT_OBSERVABILITY_PORT: u16 = 6190;

#[derive(StructOpt)]
#[structopt(
    name = "datafusion-distributed-console",
    about = "Console for monitoring DataFusion distributed workers"
)]
struct Args {
    /// Seed worker URL for auto-discovery. Connects to this worker and calls
    /// GetWorkers to discover the full cluster.
    #[structopt(long = "worker", default_value = "http://localhost:6190")]
    worker: String,

    /// Static list of worker URLs (comma-separated). When provided, skips
    /// discovery and connects directly to these workers.
    #[structopt(long = "workers", use_delimiter = true)]
    workers: Vec<String>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let args = Args::from_args();

    let (worker_urls, seed_url) = if !args.workers.is_empty() {
        // Static mode: parse provided URLs directly, no re-discovery
        let urls: Vec<Url> = args
            .workers
            .iter()
            .map(|u| Url::parse(u).expect("Invalid worker URL"))
            .collect();
        (urls, None)
    } else {
        // Discovery mode: connect to seed worker, call GetWorkers
        let seed = Url::parse(&args.worker).expect("Invalid seed worker URL");
        match discovery::discover_workers(&seed).await {
            Ok(urls) => (urls, Some(seed)),
            Err(e) => {
                eprintln!(
                    "Warning: GetWorkers discovery failed for {seed}: {e}. \
                     Falling back to seed worker only."
                );
                (vec![seed.clone()], Some(seed))
            }
        }
    };

    let mut app = App::new(worker_urls, seed_url);

    // Initialize terminal
    let mut terminal = ratatui::init();
    terminal.clear()?;

    // Run TUI loop
    let result = run_app(&mut terminal, &mut app).await;

    ratatui::restore();

    result
}

/// Main application loop
async fn run_app(terminal: &mut DefaultTerminal, app: &mut App) -> color_eyre::Result<()> {
    loop {
        app.tick().await;

        terminal.draw(|frame| ui::render(frame, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                app.handle_key_event(key);
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}
