import asyncio
import random
import time
from typing import Any, Dict, List, Optional
from tasks.base_task import BaseTask


class AdditionTask(BaseTask):
    """Tâche d'addition simple avec deux nombres."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Exécute l'addition des deux nombres fournis.
        
        Returns:
            Dict contenant le résultat de l'addition.
        """
        await self.update_progress(10, "Démarrage de l'addition")
        await asyncio.sleep(0.1)
        
        num1 = self.task_data.get("num1", 0)
        num2 = self.task_data.get("num2", 0)
        
        await self.update_progress(50, "Calcul en cours")
        await asyncio.sleep(0.2)
        
        result = num1 + num2
        
        await self.update_progress(100, "Addition terminée")
        return {
            "operation": "addition",
            "num1": num1,
            "num2": num2,
            "result": result
        }


class StringReverseTask(BaseTask):
    """Tâche d'inversion de chaîne de caractères."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Inverse la chaîne de caractères fournie.
        
        Returns:
            Dict contenant la chaîne originale et inversée.
        """
        await self.update_progress(20, "Démarrage de l'inversion")
        
        text = self.task_data.get("text", "")
        
        await self.update_progress(60, "Inversion en cours")
        await asyncio.sleep(0.3)
        
        reversed_text = text[::-1]
        
        await self.update_progress(100, "Inversion terminée")
        return {
            "operation": "string_reverse",
            "original": text,
            "reversed": reversed_text
        }


class LongRunningTask(BaseTask):
    """Tâche à exécution longue avec simulation de traitement."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Simule un traitement long avec des mises à jour de progression.
        
        Returns:
            Dict contenant les résultats du traitement.
        """
        total_steps = random.randint(5, 10)
        results = []
        
        for i in range(total_steps):
            step = i + 1
            progress = int((step / total_steps) * 100)
            
            await self.update_progress(
                progress, 
                f"Étape {step}/{total_steps} en cours"
            )
            
            # Simulation d'un traitement
            processing_time = random.uniform(0.5, 2.0)
            await asyncio.sleep(processing_time)
            
            step_result = {
                "step": step,
                "processing_time": round(processing_time, 2),
                "data": f"Résultat étape {step}"
            }
            results.append(step_result)
        
        await self.update_progress(100, "Traitement long terminé")
        return {
            "operation": "long_running",
            "total_steps": total_steps,
            "results": results
        }


class RandomFailureTask(BaseTask):
    """Tâche qui peut échouer aléatoirement (pour tests)."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Exécute une tâche avec une chance d'échec aléatoire.
        
        Returns:
            Dict contenant le résultat ou lève une exception.
        """
        await self.update_progress(30, "Démarrage de la tâche à risque")
        
        failure_chance = self.task_data.get("failure_chance", 0.3)
        
        await self.update_progress(70, "Traitement critique")
        await asyncio.sleep(1.0)
        
        if random.random() < failure_chance:
            raise Exception("Échec aléatoire simulé (comportement normal)")
        
        await self.update_progress(100, "Tâche réussie")
        return {
            "operation": "random_failure",
            "status": "success",
            "failure_chance": failure_chance
        }


class BatchProcessingTask(BaseTask):
    """Tâche de traitement par lots."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Traite une liste d'éléments en parallèle.
        
        Returns:
            Dict contenant les résultats du traitement par lots.
        """
        items = self.task_data.get("items", [])
        batch_size = self.task_data.get("batch_size", 3)
        
        await self.update_progress(10, f"Démarrage du traitement de {len(items)} éléments")
        
        results = []
        total_batches = (len(items) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(items))
            batch = items[start_idx:end_idx]
            
            await self.update_progress(
                int(((batch_num + 1) / total_batches) * 90),
                f"Traitement du lot {batch_num + 1}/{total_batches}"
            )
            
            # Traitement du lot en parallèle
            batch_results = await self._process_batch(batch)
            results.extend(batch_results)
            
            await asyncio.sleep(0.5)
        
        await self.update_progress(100, "Traitement par lots terminé")
        return {
            "operation": "batch_processing",
            "total_items": len(items),
            "batch_size": batch_size,
            "results": results
        }
    
    async def _process_batch(self, batch: List[Any]) -> List[Dict[str, Any]]:
        """Traite un lot d'éléments."""
        tasks = [self._process_item(item, idx) for idx, item in enumerate(batch)]
        return await asyncio.gather(*tasks)
    
    async def _process_item(self, item: Any, idx: int) -> Dict[str, Any]:
        """Traite un élément individuel."""
        await asyncio.sleep(random.uniform(0.1, 0.5))
        return {
            "item": item,
            "index": idx,
            "processed": True,
            "timestamp": time.time()
        }


class ExternalAPITask(BaseTask):
    """Tâche simulant un appel API externe."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Simule un appel à une API externe avec timeout et retry.
        
        Returns:
            Dict contenant la réponse simulée de l'API.
        """
        await self.update_progress(20, "Préparation de l'appel API")
        
        endpoint = self.task_data.get("endpoint", "https://api.example.com/data")
        max_retries = self.task_data.get("max_retries", 3)
        timeout = self.task_data.get("timeout", 5.0)
        
        for attempt in range(max_retries):
            try:
                await self.update_progress(
                    40 + (attempt * 20),
                    f"Tentative d'appel API {attempt + 1}/{max_retries}"
                )
                
                # Simulation d'appel API avec timeout
                result = await self._call_external_api(endpoint, timeout)
                
                await self.update_progress(100, "Appel API réussi")
                return {
                    "operation": "external_api_call",
                    "endpoint": endpoint,
                    "attempts": attempt + 1,
                    "status": "success",
                    "data": result
                }
                
            except asyncio.TimeoutError:
                if attempt == max_retries - 1:
                    raise Exception(f"Timeout après {max_retries} tentatives")
                await asyncio.sleep(1.0)
    
    async def _call_external_api(self, endpoint: str, timeout: float) -> Dict[str, Any]:
        """Simule un appel API externe."""
        # Simulation de latence réseau
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        # Simulation d'échec aléatoire
        if random.random() < 0.2:
            raise asyncio.TimeoutError("Timeout simulé")
        
        return {
            "endpoint": endpoint,
            "status_code": 200,
            "response": {
                "data": f"Réponse simulée de {endpoint}",
                "timestamp": time.time(),
                "items": [f"item_{i}" for i in range(random.randint(1, 5))]
            }
        }


class FileProcessingTask(BaseTask):
    """Tâche de traitement de fichier (simulation)."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Simule le traitement d'un fichier.
        
        Returns:
            Dict contenant les métadonnées du traitement.
        """
        await self.update_progress(10, "Initialisation du traitement de fichier")
        
        filename = self.task_data.get("filename", "document.txt")
        operation = self.task_data.get("operation", "analyze")
        
        await self.update_progress(30, "Ouverture du fichier")
        await asyncio.sleep(0.5)
        
        await self.update_progress(50, f"Exécution de l'opération: {operation}")
        await asyncio.sleep(1.0)
        
        # Simulation de traitement
        file_size = random.randint(1024, 10240)
        lines = random.randint(10, 100)
        
        await self.update_progress(80, "Génération du rapport")
        await asyncio.sleep(0.8)
        
        await self.update_progress(100, "Traitement de fichier terminé")
        return {
            "operation": "file_processing",
            "filename": filename,
            "file_operation": operation,
            "metadata": {
                "size_bytes": file_size,
                "lines": lines,
                "processed_at": time.time(),
                "status": "completed"
            }
        }


class DataTransformationTask(BaseTask):
    """Tâche de transformation de données."""
    
    async def execute(self) -> Dict[str, Any]:
        """
        Transforme des données selon des règles spécifiques.
        
        Returns:
            Dict contenant les données transformées.
        """
        await self.update_progress(10, "Démarrage de la transformation")
        
        data = self.task_data.get("data", {})
        transformation_rules = self.task_data.get("rules", {})
        
        await self.update_progress(40, "Application des règles de transformation")
        await asyncio.sleep(1.0)
        
        transformed_data = self._apply_transformations(data, transformation_rules)
        
        await self.update_progress(80, "Validation des résultats")
        await asyncio.sleep(0.5)
        
        await self.update_progress(100, "Transformation terminée")
        return {
            "operation": "data_transformation",
            "original_data": data,
            "transformation_rules": transformation_rules,
            "transformed_data": transformed_data
        }
    
    def _apply_transformations(self, data: Dict, rules: Dict) -> Dict:
        """Applique les transformations aux données."""
        result = data.copy()
        
        for key, rule in rules.items():
            if key in result:
                if rule == "uppercase" and isinstance(result[key], str):
                    result[key] = result[key].upper()
                elif rule == "increment" and isinstance(result[key], (int, float)):
                    result[key] = result[key] + 1
                elif rule == "reverse" and isinstance(result[key], str):
                    result[key] = result[key][::-1]
        
        return result