"""Command-line interface for Polymarket Copy Trading Tool."""
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from datetime import datetime
from bitquery_client import BitqueryClient
from position_tracker import PositionTracker
from config import Config

console = Console(force_terminal=True, width=None)  # Allow unlimited width to prevent truncation

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

if __name__ == '__main__':
    cli()

