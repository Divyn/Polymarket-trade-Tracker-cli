"""Command-line interface for Polymarket Copy Trading Tool."""
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from datetime import datetime
from typing import Optional

from bitquery_client import BitqueryClient
from position_tracker import PositionTracker
from question_analyzer import QuestionAnalyzer
from config import Config

console = Console(force_terminal=True, width=None)  # Allow unlimited width to prevent truncation


def _format_time(value: datetime | None) -> str:
    if not value:
        return "—"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _shorten(value: str | None, length: int = 12) -> str:
    if not value:
        return "—"
    if len(value) <= length:
        return value
    return f"{value[:length//2]}…{value[-length//2:]}"

@click.group()
def cli():
    """Polymarket Copy Trading Tool - Monitor and copy trades from CTF Exchange."""
    try:
        Config.validate()
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--address', '-a', required=True, help='Trader wallet address to monitor')
@click.option('--limit', '-l', default=20, help='Number of positions to fetch')
def monitor(address: str, limit: int):
    """Monitor trades from a specific trader address."""
    console.print(f"[cyan]Monitoring trader: {address}[/cyan]")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        positions = tracker.track_trader(address, limit=limit)
        
        if not positions:
            console.print("[yellow]No positions found for this trader.[/yellow]")
            return
        
        # Display positions
        table = Table(title=f"Positions from {address}", box=box.ROUNDED, expand=True, show_header=True, width=None)
        table.add_column("Maker", style="cyan", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Taker", style="cyan", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Asset ID", style="green", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Amount", style="yellow", justify="right", min_width=10, max_width=10)
        table.add_column("Price", style="magenta", justify="right", min_width=10, max_width=10)
        table.add_column("Time", style="blue", min_width=19, max_width=19)
        table.add_column("TX Hash", style="dim", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        
        for pos in positions:
            table.add_row(
                pos.maker_address,
                pos.taker_address,
                pos.asset_id,
                f"{pos.amount:.4f}",
                f"{pos.price:.4f}",
                pos.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                pos.tx_hash
            )
        
        console.print(table)
        
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--address', '-a', required=True, help='Maker address to follow (e.g., 0xfa323e40632edca701abc6c8cf1f82175909bd9a)')
@click.option('--limit', '-l', default=20, help='Number of positions to fetch')
@click.option('--since-hours', '-h', type=int, default=None, help='Filter trades from the last N hours')
def follow_trader(address: str, limit: int, since_hours: Optional[int]):
    """Follow trades where a specific address is the maker.
    
    This command filters OrderFilled events where the maker address
    matches the provided address, allowing you to track trades where
    a specific trader is making orders.
    """
    console.print(f"[cyan]Following maker: {address}[/cyan]")
    if since_hours:
        console.print(f"[cyan]Filtering trades from last {since_hours} hours[/cyan]")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        positions = tracker.follow_trader_positions(
            maker_address=address,
            limit=limit,
            since_hours=since_hours
        )
        
        if not positions:
            console.print("[yellow]No positions found for this maker address.[/yellow]")
            return
        
        # Display positions
        table = Table(title=f"Positions where {address} is Maker", box=box.ROUNDED, expand=True, show_header=True, width=None)
        table.add_column("Maker", style="cyan", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Taker", style="cyan", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Asset ID", style="green", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Amount", style="yellow", justify="right", min_width=10, max_width=10)
        table.add_column("Price", style="magenta", justify="right", min_width=10, max_width=10)
        table.add_column("Time", style="blue", min_width=19, max_width=19)
        table.add_column("TX Hash", style="dim", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        
        for pos in positions:
            table.add_row(
                pos.maker_address,
                pos.taker_address,
                pos.asset_id,
                f"{pos.amount:.4f}",
                f"{pos.price:.4f}",
                pos.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                pos.tx_hash
            )
        
        console.print(table)
        
        # Display summary
        total_volume = sum(p.amount * p.price for p in positions)
        unique_assets = len(set(p.asset_id for p in positions))
        console.print()
        console.print(Panel(
            f"[green]Total Positions:[/green] {len(positions)}\n"
            f"[green]Total Volume:[/green] ${total_volume:.2f}\n"
            f"[green]Unique Assets:[/green] {unique_assets}",
            title="Summary",
            border_style="green"
        ))
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--limit', '-l', default=20, help='Number of trades to fetch')
@click.option('--asset-id', '-a', help='Filter by asset ID')
def list_trades(limit: int, asset_id: str):
    """List recent trades from Polymarket CTF Exchange."""
    console.print("[cyan]Fetching recent trades...[/cyan]")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        if asset_id:
            positions = tracker.get_positions_by_asset(asset_id)
        else:
            positions = tracker.get_recent_positions(limit=limit)
        
        if not positions:
            console.print("[yellow]No trades found.[/yellow]")
            return
        
        table = Table(title="Recent Polymarket Trades", box=box.ROUNDED, expand=True, show_header=True, width=None)
        table.add_column("Maker", style="cyan", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Taker", style="cyan", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Asset ID", style="green", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        table.add_column("Amount", style="yellow", justify="right", min_width=10, max_width=10)
        table.add_column("Price", style="magenta", justify="right", min_width=10, max_width=10)
        table.add_column("Time", style="blue", min_width=19, max_width=19)
        table.add_column("TX Hash", style="dim", no_wrap=False, overflow="fold", min_width=20, max_width=20)
        
        for pos in positions:
            table.add_row(
                pos.maker_address,
                pos.taker_address,
                pos.asset_id,
                f"{pos.amount:.4f}",
                f"{pos.price:.4f}",
                pos.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                pos.tx_hash
            )
        
        console.print(table)
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--asset-id', '-a', required=True, help='Asset ID to copy')
@click.option('--fixed-amount', '-f', default=None, type=float, help='Fixed USD amount to spend (default: 0.001 from config)')
@click.option('--execute', is_flag=True, help='Actually execute trade (requires private key)')
@click.option('--skip-question-details', is_flag=True, help='Skip querying question details for faster execution')
def copy_position(asset_id: str, fixed_amount: float, execute: bool, skip_question_details: bool):
    """Copy a specific position by asset ID."""
    if not skip_question_details:
        console.print(f"[cyan]Fetching question details for asset: {asset_id}[/cyan]")
    else:
        console.print(f"[cyan]Copying position for asset: {asset_id}[/cyan]")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        # First, get and display question details (ancillary_data) unless skipped
        if not skip_question_details:
            console.print("[cyan]Fetching market question details...[/cyan]")
            ancillary_data = tracker.get_question_details(asset_id)
            
            if ancillary_data:
                # Display ancillary_data in a nice format
                question_panel = Panel(
                    f"[bold cyan]Ancillary Data:[/bold cyan]\n{ancillary_data}",
                    title="Market Question Details",
                    border_style="green"
                )
                console.print(question_panel)
                console.print()  # Add spacing
            else:
                console.print("[yellow]Could not fetch question details. Continuing with position copy...[/yellow]")
                console.print()
        
        # Now get positions
        console.print(f"[cyan]Fetching position data for asset: {asset_id}[/cyan]")
        positions = tracker.get_positions_by_asset(asset_id)
        
        if not positions:
            console.print(f"[yellow]No positions found for asset ID: {asset_id}[/yellow]")
            return
        
        # Use the most recent position
        latest_position = max(positions, key=lambda p: p.timestamp)
        
        console.print(Panel(
            f"[green]Asset ID:[/green] {latest_position.asset_id}\n"
            # f"[green]Trader:[/green] {latest_position.trader_address}\n"
            f"[green]Maker:[/green] {latest_position.maker_address}\n"
            f"[green]Taker:[/green] {latest_position.taker_address}\n"
            f"[green]Amount:[/green] {latest_position.amount:.4f}\n"
            f"[green]Price:[/green] {latest_position.price:.4f}\n"
            f"[green]Direction:[/green] {latest_position.direction}\n"
            f"[green]Time:[/green] {latest_position.timestamp}",
            title="Position Details",
            border_style="cyan"
        ))
        
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--address', '-a', required=True, help='Trader wallet address')
def trader_summary(address: str):
    """Get summary statistics for a trader."""
    console.print(f"[cyan]Analyzing trader: {address}[/cyan] in last 10k trades")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        # Track recent positions first
        positions = tracker.track_trader(address, limit=50)
        
        # Use the positions returned by track_trader directly
        summary = tracker.get_trader_summary(address, positions=positions)
        
        # Print address separately to ensure it's not truncated
        console.print(f"[green]Trader Address:[/green] {address}")
        console.print()
        
        console.print(Panel(
            f"[green]Total Positions:[/green] {summary['total_positions']}\n"
            f"[green]Total Volume:[/green] {summary['total_volume']:.2f}\n"
            f"[green]Average Price:[/green] {summary['avg_price']:.4f}\n"
            f"[green]Unique Assets:[/green] {summary['unique_assets']}",
            title="Trader Summary",
            border_style="cyan"
        ))
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--asset-id', '-a', required=True, help='Asset ID')
def market_price(asset_id: str):
    """Get current market price for an asset."""
    console.print(f"[cyan]Calculating market price for asset: {asset_id}[/cyan]")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        price = tracker.calculate_market_price(asset_id)
        
        if price:
            console.print(Panel(
                f"[green]Asset ID:[/green] {asset_id}\n"
                f"[green]Current Price:[/green] {price:.4f}",
                title="Market Price",
                border_style="green"
            ))
        else:
            console.print(f"[yellow]No recent trades found for this asset.[/yellow]")
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()

@cli.command()
@click.option('--limit', '-l', default=20000, help='Number of trades to fetch (default: 20000)')
@click.option('--top-traders', '-t', default=20, help='Number of top traders to display (default: 20)')
@click.option('--top-assets', '-a', default=20, help='Number of top assets to display (default: 20)')
def top_traders(limit: int, top_traders: int, top_assets: int):
    """Display top traders and asset IDs based on trading volume."""
    console.print(f"[cyan]Analyzing top traders and assets from {limit} trades...[/cyan]")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        results = tracker.get_top_traders_and_assets(
            limit=limit,
            top_traders_count=top_traders,
            top_assets_count=top_assets
        )
        
        console.print(f"\n[green]Analyzed {results['total_parsed']} trades from {results['total_trades']} events[/green]\n")
        
        # Display top traders
        if results['traders']:
            traders_table = Table(
                title=f"Top {len(results['traders'])} Traders by Volume",
                box=box.ROUNDED,
                expand=True,
                show_header=True,
                width=None
            )
            traders_table.add_column("Rank", style="cyan", justify="right", width=6)
            traders_table.add_column("Address", style="cyan", no_wrap=False, overflow="fold", min_width=42, max_width=42)
            traders_table.add_column("Total Volume (USD)", style="green", justify="right", min_width=18)
            traders_table.add_column("Trades", style="yellow", justify="right", min_width=10)
            traders_table.add_column("Unique Assets", style="magenta", justify="right", min_width=12)
            traders_table.add_column("Avg Price", style="blue", justify="right", min_width=12)
            
            for idx, trader in enumerate(results['traders'], 1):
                traders_table.add_row(
                    str(idx),
                    trader['address'],
                    f"${trader['total_volume']:.2f}",
                    str(trader['total_trades']),
                    str(trader['unique_assets_count']),
                    f"{trader['avg_price']:.4f}"
                )
            
            console.print(traders_table)
        else:
            console.print("[yellow]No traders found.[/yellow]")
        
        console.print()  # Add spacing
        
        # Display top assets
        if results['assets']:
            assets_table = Table(
                title=f"Top {len(results['assets'])} Assets by Volume",
                box=box.ROUNDED,
                expand=True,
                show_header=True,
                width=None
            )
            assets_table.add_column("Rank", style="cyan", justify="right", width=6)
            assets_table.add_column("Asset ID", style="cyan", no_wrap=False, overflow="fold", min_width=20, max_width=50)
            assets_table.add_column("Total Volume (USD)", style="green", justify="right", min_width=18)
            assets_table.add_column("Trades", style="yellow", justify="right", min_width=10)
            assets_table.add_column("Unique Traders", style="magenta", justify="right", min_width=14)
            assets_table.add_column("Avg Price", style="blue", justify="right", min_width=12)
            
            for idx, asset in enumerate(results['assets'], 1):
                assets_table.add_row(
                    str(idx),
                    asset['asset_id'],
                    f"${asset['total_volume']:.2f}",
                    str(asset['total_trades']),
                    str(asset['unique_traders_count']),
                    f"{asset['avg_price']:.4f}"
                )
            
            console.print(assets_table)
        else:
            console.print("[yellow]No assets found.[/yellow]")
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        import traceback
        traceback.print_exc()
        raise click.Abort()


@cli.command(name="analyze-questions")
@click.option("--limit", "-l", default=25, show_default=True, help="Number of questions to analyze.")
@click.option("--max-keywords", "-k", default=6, show_default=True, help="Keywords to surface per question.")
@click.option("--show-text", is_flag=True, help="Print full ancillary text after the summary table.")
@click.option(
    "--log-file",
    "-o",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    help="Also write a plain-text (no color codes) copy of the output to this file.",
)
def analyze_questions(limit: int, max_keywords: int, show_text: bool, log_file: str | None):
    """Inspect UMA QuestionInitialized events and decode ancillaryData."""
    client = BitqueryClient()
    analyzer = QuestionAnalyzer(max_keywords=max_keywords)

    # Optional second console that writes plain text to a file
    log_console = None
    log_fp = None
    if log_file:
        # No colors, no terminal control codes → human-readable log
        log_fp = open(log_file, "w", encoding="utf-8")
        log_console = Console(
            file=log_fp,
            force_terminal=False,
            color_system=None,
            width=None,
        )

    def _print(*args, **kwargs):
        """Print to main console and (optionally) to log console."""
        console.print(*args, **kwargs)
        if log_console is not None:
            log_console.print(*args, **kwargs)

    try:
        _print(f"[cyan]Fetching last {limit} QuestionInitialized events...[/cyan]")
        events = client.get_recent_question_initialized_events(limit=limit)

        if not events:
            _print("[yellow]No QuestionInitialized events returned by Bitquery.[/yellow]")
            raise click.Abort()

        analyses = analyzer.analyze_events(events)
        if not analyses:
            _print("[yellow]No events contained ancillaryData to decode.[/yellow]")
            raise click.Abort()

        table = Table(title="Question Analyzer", expand=True)
        table.add_column("Time (UTC)", style="blue")
        table.add_column("Question ID", style="cyan")
        table.add_column("Topics", style="magenta")
        table.add_column("Keywords", style="green")
        table.add_column("Tx Hash", style="dim")

        for analysis in analyses:
            table.add_row(
                _format_time(analysis.block_time),
                _shorten(analysis.question_id),
                ", ".join(analysis.topics) if analysis.topics else "General",
                ", ".join(analysis.keywords) if analysis.keywords else "—",
                _shorten(analysis.tx_hash, length=16),
            )

        _print(table)

        if show_text:
            _print()
            for analysis in analyses:
                subtitle = []
                if analysis.question_id:
                    subtitle.append(f"QID: {analysis.question_id}")
                if analysis.condition_id:
                    subtitle.append(f"CID: {analysis.condition_id}")
                if analysis.block_number:
                    subtitle.append(f"Block: {analysis.block_number}")
                if analysis.tx_hash:
                    subtitle.append(f"Tx: {analysis.tx_hash}")

                panel_title = " | ".join(subtitle) if subtitle else "Ancillary Data"
                _print(
                    Panel(
                        analysis.ancillary_text or "[dim]No ancillary text[/dim]",
                        title=panel_title,
                        border_style="cyan",
                    )
                )
    finally:
        if log_fp is not None:
            log_fp.close()


@cli.command(name="get-orderbook")
@click.option('--asset-id', '-a', required=True, help='Asset ID')
@click.option('--limit', '-l', default=200, help='Number of positions to fetch')
def get_orderbook(asset_id: str, limit: int):
    """Reconstruct recent orderbook snapshot from completed trades for a specific asset.
    
    This shows what the orderbook looked like based on recent OrderFilled events.
    The orderbook is reconstructed chronologically, showing depth at each price level.
    """
    console.print(f"[cyan]Reconstructing recent orderbook for asset: {asset_id}[/cyan]")
    console.print("[dim]Note: This is reconstructed from completed trades, not live open orders[/dim]")
    
    client = BitqueryClient()
    tracker = PositionTracker(client)
    
    try:
        orderbook = tracker.get_orderbook(asset_id, limit=limit)
        
        if not orderbook.get('bids') and not orderbook.get('asks'):
            console.print(f"[yellow]No orderbook data found for asset ID: {asset_id}[/yellow]")
            return
        
        # Display snapshot time if available
        if orderbook.get('snapshot_time'):
            snapshot_str = _format_time(orderbook['snapshot_time'])
            console.print(f"[dim]Orderbook snapshot as of: {snapshot_str}[/dim]")
            console.print()
        
        # Display bids table
        if orderbook.get('bids'):
            bids_table = Table(
                title=f"Bids (Buy Orders) - Asset: {asset_id}",
                box=box.ROUNDED,
                expand=True,
                show_header=True,
                width=None
            )
            bids_table.add_column("Price", style="green", justify="right", min_width=12)
            bids_table.add_column("Amount", style="yellow", justify="right", min_width=12)
            bids_table.add_column("Count", style="cyan", justify="right", min_width=8)
            
            for bid in orderbook['bids']:
                bids_table.add_row(
                    f"{bid['price']:.4f}",
                    f"{bid['amount']:.4f}",
                    str(bid['count'])
                )
            
            console.print(bids_table)
        else:
            console.print("[yellow]No bids found.[/yellow]")
        
        console.print()  # Add spacing
        
        # Display asks table
        if orderbook.get('asks'):
            asks_table = Table(
                title=f"Asks (Sell Orders) - Asset: {asset_id}",
                box=box.ROUNDED,
                expand=True,
                show_header=True,
                width=None
            )
            asks_table.add_column("Price", style="red", justify="right", min_width=12)
            asks_table.add_column("Amount", style="yellow", justify="right", min_width=12)
            asks_table.add_column("Count", style="cyan", justify="right", min_width=8)
            
            for ask in orderbook['asks']:
                asks_table.add_row(
                    f"{ask['price']:.4f}",
                    f"{ask['amount']:.4f}",
                    str(ask['count'])
                )
            
            console.print(asks_table)
        else:
            console.print("[yellow]No asks found.[/yellow]")
        
        # Display summary
        console.print()
        summary_lines = []
        if orderbook.get('total_events') is not None:
            summary_lines.append(f"[green]Total Events:[/green] {orderbook['total_events']}")
        if orderbook.get('total_positions') is not None:
            summary_lines.append(f"[green]Total Positions:[/green] {orderbook['total_positions']}")
        if orderbook.get('snapshot_time'):
            summary_lines.append(f"[green]Snapshot Time:[/green] {_format_time(orderbook['snapshot_time'])}")
        
        if summary_lines:
            console.print(Panel(
                "\n".join(summary_lines),
                title="Orderbook Summary",
                border_style="cyan"
            ))
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort()
        
if __name__ == '__main__':
    cli()

