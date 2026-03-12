import json
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.syntax import Syntax
    from rich.panel import Panel
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from core.task import Task, TaskStatus
from core.queue_manager import QueueType
from utils.logger import get_logger

logger = get_logger(__name__)

class CLIFormatter:
    """Formateur pour la sortie CLI avec support conditionnel pour rich"""
    
    def __init__(self, use_color: bool = True, output_format: str = "table"):
        self.use_color = use_color and RICH_AVAILABLE
        self.output_format = output_format
        self.console = Console() if RICH_AVAILABLE else None
        
    def format_task(self, task: Union[Task, Dict], detailed: bool = False) -> str:
        """Formate une tâche pour l'affichage"""
        if isinstance(task, dict):
            task_data = task
        else:
            task_data = task.to_dict()
        
        if self.output_format == "json":
            return json.dumps(task_data, indent=2, default=str)
        
        if detailed:
            return self._format_task_detailed(task_data)
        else:
            return self._format_task_simple(task_data)
    
    def _format_task_simple(self, task_data: Dict) -> str:
        """Format simple d'une tâche (une ligne)"""
        task_id = task_data.get('task_id', 'N/A')
        status = task_data.get('status', 'UNKNOWN')
        created_at = task_data.get('created_at')
        queue = task_data.get('queue_name', 'default')
        
        if created_at:
            if isinstance(created_at, str):
                created_str = created_at
            else:
                created_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
        else:
            created_str = 'N/A'
        
        if self.use_color:
            status_color = self._get_status_color(status)
            status_text = Text(status, style=status_color)
            return f"{task_id[:8]}... | {status_text} | {created_str} | {queue}"
        else:
            return f"{task_id[:8]}... | {status} | {created_str} | {queue}"
    
    def _format_task_detailed(self, task_data: Dict) -> str:
        """Format détaillé d'une tâche"""
        if self.use_color:
            return self._format_task_detailed_rich(task_data)
        else:
            return self._format_task_detailed_plain(task_data)
    
    def _format_task_detailed_rich(self, task_data: Dict) -> str:
        """Format détaillé avec rich"""
        task_id = task_data.get('task_id', 'N/A')
        status = task_data.get('status', 'UNKNOWN')
        created_at = task_data.get('created_at')
        updated_at = task_data.get('updated_at')
        queue_name = task_data.get('queue_name', 'default')
        priority = task_data.get('priority', 0)
        task_type = task_data.get('task_type', 'unknown')
        result = task_data.get('result')
        error = task_data.get('error')
        metadata = task_data.get('metadata', {})
        
        # Création du tableau
        table = Table(title=f"Task Details: {task_id}", box=box.ROUNDED)
        table.add_column("Field", style="cyan", no_wrap=True)
        table.add_column("Value", style="white")
        
        # Ajout des lignes
        status_color = self._get_status_color(status)
        table.add_row("Task ID", task_id)
        table.add_row("Status", Text(status, style=status_color))
        table.add_row("Type", task_type)
        table.add_row("Queue", queue_name)
        table.add_row("Priority", str(priority))
        
        if created_at:
            created_str = created_at if isinstance(created_at, str) else created_at.strftime('%Y-%m-%d %H:%M:%S')
            table.add_row("Created", created_str)
        
        if updated_at:
            updated_str = updated_at if isinstance(updated_at, str) else updated_at.strftime('%Y-%m-%d %H:%M:%S')
            table.add_row("Updated", updated_str)
        
        if result is not None:
            result_str = str(result)[:100] + "..." if len(str(result)) > 100 else str(result)
            table.add_row("Result", result_str)
        
        if error:
            error_str = str(error)[:200] + "..." if len(str(error)) > 200 else str(error)
            table.add_row("Error", Text(error_str, style="red"))
        
        if metadata:
            metadata_str = json.dumps(metadata, indent=2)
            if len(metadata_str) > 200:
                metadata_str = metadata_str[:200] + "..."
            table.add_row("Metadata", metadata_str)
        
        # Capture la sortie
        output = []
        self.console.print(table, end="")
        return str(table)
    
    def _format_task_detailed_plain(self, task_data: Dict) -> str:
        """Format détaillé sans rich"""
        lines = []
        lines.append(f"Task ID: {task_data.get('task_id', 'N/A')}")
        lines.append(f"Status: {task_data.get('status', 'UNKNOWN')}")
        lines.append(f"Type: {task_data.get('task_type', 'unknown')}")
        lines.append(f"Queue: {task_data.get('queue_name', 'default')}")
        lines.append(f"Priority: {task_data.get('priority', 0)}")
        
        if task_data.get('created_at'):
            created_at = task_data['created_at']
            created_str = created_at if isinstance(created_at, str) else created_at.strftime('%Y-%m-%d %H:%M:%S')
            lines.append(f"Created: {created_str}")
        
        if task_data.get('updated_at'):
            updated_at = task_data['updated_at']
            updated_str = updated_at if isinstance(updated_at, str) else updated_at.strftime('%Y-%m-%d %H:%M:%S')
            lines.append(f"Updated: {updated_str}")
        
        if task_data.get('result') is not None:
            lines.append(f"Result: {task_data['result']}")
        
        if task_data.get('error'):
            lines.append(f"Error: {task_data['error']}")
        
        if task_data.get('metadata'):
            lines.append(f"Metadata: {json.dumps(task_data['metadata'], indent=2)}")
        
        return "\n".join(lines)
    
    def format_queue_status(self, queue_data: Dict, detailed: bool = False) -> str:
        """Formate les informations de la file d'attente"""
        if self.output_format == "json":
            return json.dumps(queue_data, indent=2, default=str)
        
        if detailed:
            return self._format_queue_detailed(queue_data)
        else:
            return self._format_queue_simple(queue_data)
    
    def _format_queue_simple(self, queue_data: Dict) -> str:
        """Format simple de la file d'attente"""
        queue_name = queue_data.get('name', 'unknown')
        queue_type = queue_data.get('type', 'FIFO')
        size = queue_data.get('size', 0)
        max_size = queue_data.get('max_size', 0)
        
        if self.use_color:
            size_style = "green" if size < max_size * 0.8 else "yellow" if size < max_size * 0.95 else "red"
            size_text = Text(f"{size}/{max_size}", style=size_style)
            return f"{queue_name} ({queue_type}): {size_text} tasks"
        else:
            return f"{queue_name} ({queue_type}): {size}/{max_size} tasks"
    
    def _format_queue_detailed(self, queue_data: Dict) -> str:
        """Format détaillé de la file d'attente"""
        if self.use_color:
            return self._format_queue_detailed_rich(queue_data)
        else:
            return self._format_queue_detailed_plain(queue_data)
    
    def _format_queue_detailed_rich(self, queue_data: Dict) -> str:
        """Format détaillé avec rich"""
        queue_name = queue_data.get('name', 'unknown')
        queue_type = queue_data.get('type', 'FIFO')
        size = queue_data.get('size', 0)
        max_size = queue_data.get('max_size', 0)
        pending = queue_data.get('pending', 0)
        processing = queue_data.get('processing', 0)
        completed = queue_data.get('completed', 0)
        failed = queue_data.get('failed', 0)
        
        # Calcul des pourcentages
        total_processed = completed + failed
        success_rate = (completed / total_processed * 100) if total_processed > 0 else 0
        
        # Création du tableau
        table = Table(title=f"Queue Status: {queue_name}", box=box.ROUNDED)
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="white")
        table.add_column("Details", style="dim")
        
        # Ajout des lignes
        table.add_row("Type", queue_type, "")
        
        size_style = "green" if size < max_size * 0.8 else "yellow" if size < max_size * 0.95 else "red"
        table.add_row("Size", Text(f"{size}/{max_size}", style=size_style), f"{size/max_size*100:.1f}%" if max_size > 0 else "N/A")
        
        table.add_row("Pending", str(pending), "")
        table.add_row("Processing", str(processing), "")
        table.add_row("Completed", str(completed), "")
        
        failed_style = "red" if failed > 0 else "dim"
        table.add_row("Failed", Text(str(failed), style=failed_style), "")
        
        success_style = "green" if success_rate >= 90 else "yellow" if success_rate >= 70 else "red"
        table.add_row("Success Rate", Text(f"{success_rate:.1f}%", style=success_style), f"{completed}/{total_processed}")
        
        return str(table)
    
    def _format_queue_detailed_plain(self, queue_data: Dict) -> str:
        """Format détaillé sans rich"""
        lines = []
        lines.append(f"Queue: {queue_data.get('name', 'unknown')}")
        lines.append(f"Type: {queue_data.get('type', 'FIFO')}")
        lines.append(f"Size: {queue_data.get('size', 0)}/{queue_data.get('max_size', 0)}")
        lines.append(f"Pending: {queue_data.get('pending', 0)}")
        lines.append(f"Processing: {queue_data.get('processing', 0)}")
        lines.append(f"Completed: {queue_data.get('completed', 0)}")
        lines.append(f"Failed: {queue_data.get('failed', 0)}")
        
        total_processed = queue_data.get('completed', 0) + queue_data.get('failed', 0)
        if total_processed > 0:
            success_rate = (queue_data.get('completed', 0) / total_processed) * 100
            lines.append(f"Success Rate: {success_rate:.1f}%")
        
        return "\n".join(lines)
    
    def format_tasks_table(self, tasks: List[Union[Task, Dict]], 
                          columns: Optional[List[str]] = None) -> str:
        """Formate une liste de tâches en tableau"""
        if not tasks:
            return "No tasks found."
        
        if self.output_format == "json":
            task_list = [t.to_dict() if isinstance(t, Task) else t for t in tasks]
            return json.dumps(task_list, indent=2, default=str)
        
        if self.use_color:
            return self._format_tasks_table_rich(tasks, columns)
        else:
            return self._format_tasks_table_plain(tasks, columns)
    
    def _format_tasks_table_rich(self, tasks: List[Union[Task, Dict]], 
                                columns: Optional[List[str]] = None) -> str:
        """Format de tableau avec rich"""
        if columns is None:
            columns = ["task_id", "status", "task_type", "created_at", "queue_name"]
        
        table = Table(title="Tasks", box=box.ROUNDED)
        
        # Définition des colonnes
        column_config = {
            "task_id": ("Task ID", "cyan"),
            "status": ("Status", "white"),
            "task_type": ("Type", "dim"),
            "created_at": ("Created", "dim"),
            "updated_at": ("Updated", "dim"),
            "queue_name": ("Queue", "blue"),
            "priority": ("Priority", "yellow"),
        }
        
        for col in columns:
            if col in column_config:
                name, style = column_config[col]
                table.add_column(name, style=style)
            else:
                table.add_column(col.replace("_", " ").title(), style="white")
        
        # Ajout des données
        for task in tasks:
            if isinstance(task, Task):
                task_data = task.to_dict()
            else:
                task_data = task
            
            row = []
            for col in columns:
                value = task_data.get(col)
                if value is None:
                    row.append("")
                elif col == "status":
                    status_color = self._get_status_color(value)
                    row.append(Text(str(value), style=status_color))
                elif col == "created_at" or col == "updated_at":
                    if isinstance(value, datetime):
                        row.append(value.strftime('%Y-%m-%d %H:%M'))
                    else:
                        row.append(str(value))
                elif col == "task_id":
                    row.append(str(value)[:12] + "..." if len(str(value)) > 12 else str(value))
                else:
                    row.append(str(value))
            
            table.add_row(*row)
        
        return str(table)
    
    def _format_tasks_table_plain(self, tasks: List[Union[Task, Dict]], 
                                 columns: Optional[List[str]] = None) -> str:
        """Format de tableau sans rich"""
        if columns is None:
            columns = ["task_id", "status", "task_type", "created_at", "queue_name"]
        
        # Préparation des en-têtes
        headers = [col.replace("_", " ").title() for col in columns]
        
        # Préparation des données
        rows = []
        for task in tasks:
            if isinstance(task, Task):
                task_data = task.to_dict()
            else:
                task_data = task
            
            row = []
            for col in columns:
                value = task_data.get(col)
                if value is None:
                    row.append("")
                elif col == "created_at" or col == "updated_at":
                    if isinstance(value, datetime):
                        row.append(value.strftime('%Y-%m-%d %H:%M'))
                    else:
                        row.append(str(value))
                elif col == "task_id":
                    row.append(str(value)[:12] + "..." if len(str(value)) > 12 else str(value))
                else:
                    row.append(str(value))
            rows.append(row)
        
        # Calcul des largeurs de colonnes
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))
        
        # Construction du tableau
        lines = []
        
        # Ligne de séparation
        sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
        lines.append(sep)
        
        # En-têtes
        header_cells = [f" {headers[i]:<{col_widths[i]}} " for i in range(len(headers))]
        lines.append("|" + "|".join(header_cells) + "|")
        lines.append(sep)
        
        # Données
        for row in rows:
            cells = [f" {str(row[i]):<{col_widths[i]}} " for i in range(len(row))]
            lines.append("|" + "|".join(cells) + "|")
        
        lines.append(sep)
        
        return "\n".join(lines)
    
    def format_success(self, message: str) -> str:
        """Formate un message de succès"""
        if self.use_color and self.console:
            return f"[green]✓[/green] {message}"
        else:
            return f"✓ {message}"
    
    def format_error(self, message: str) -> str:
        """Formate un message d'erreur"""
        if self.use_color and self.console:
            return f"[red]✗[/red] {message}"
        else:
            return f"✗ {message}"
    
    def format_warning(self, message: str) -> str:
        """Formate un message d'avertissement"""
        if self.use_color and self.console:
            return f"[yellow]⚠[/yellow] {message}"
        else:
            return f"⚠ {message}"
    
    def format_info(self, message: str) -> str:
        """Formate un message d'information"""
        if self.use_color and self.console:
            return f"[blue]ℹ[/blue] {message}"
        else:
            return f"ℹ {message}"
    
    def format_progress_bar(self, current: int, total: int, 
                           description: str = "Processing") -> str:
        """Formate une barre de progression"""
        if self.use_color and self.console and RICH_AVAILABLE:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task(description, total=total)
                progress.update(task, completed=current)
                return ""
        else:
            if total > 0:
                percent = (current / total) * 100
                return f"{description}: {current}/{total} ({percent:.1f}%)"
            else:
                return f"{description}: {current}"
    
    def _get_status_color(self, status: str) -> str:
        """Retourne la couleur appropriée pour un statut"""
        status_colors = {
            TaskStatus.PENDING: "yellow",
            TaskStatus.QUEUED: "blue",
            TaskStatus.PROCESSING: "cyan",
            TaskStatus.COMPLETED: "green",
            TaskStatus.FAILED: "red",
            TaskStatus.CANCELLED: "dim",
            TaskStatus.TIMEOUT: "red",
        }
        return status_colors.get(status, "white")

def print_formatted(data: Any, formatter: Optional[CLIFormatter] = None, **kwargs):
    """Fonction utilitaire pour afficher des données formatées"""
    if formatter is None:
        formatter = CLIFormatter()
    
    if isinstance(data, list):
        if all(isinstance(item, (Task, dict)) for item in data):
            print(formatter.format_tasks_table(data, **kwargs))
        else:
            print(formatter.format_tasks_table([{"value": str(d)} for d in data], 
                                              columns=["value"]))
    elif isinstance(data, dict):
        if 'name' in data and 'type' in data:  # Probablement une file d'attente
            print(formatter.format_queue_status(data, **kwargs))
        elif 'task_id' in data or 'status' in data:  # Probablement une tâche
            print(formatter.format_task(data, **kwargs))
        else:
            print(json.dumps(data, indent=2, default=str))
    elif isinstance(data, Task):
        print(formatter.format_task(data, **kwargs))
    else:
        print(str(data))

def create_formatter(output_format: str = "table", color: bool = True) -> CLIFormatter:
    """Crée un formateur CLI"""
    return CLIFormatter(use_color=color, output_format=output_format)