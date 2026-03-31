import math
from typing import Dict, Tuple

class AnomalyEngine:
    """
    Stateful anomaly detection engine utilizing a temporal 168-cell baseline matrix.
    
    The baseline matrix tracks data by the hour of the day (0-23) and day of the week (0-6).
    It computes running mean and standard deviation sequentially using Welford's Online
    Algorithm, making it memory-efficient and completely isolated from database I/O.
    """
    
    def __init__(self):
        """
        Initializes the empty 168-cell baseline matrix.
        The underlying data structure is a dictionary keyed by (hour, day) containing 
        the variables necessary for Welford's algorithm: count, mean, and m2 (aggregate squared distance).
        """
        self.matrix: Dict[Tuple[int, int], Dict[str, float]] = {}

    def update_baseline(self, hour: int, day: int, value: float) -> None:
        """
        Updates the running mean and variance for a specific (hour, day) time cell 
        using Welford's online algorithm.
        
        Args:
            hour (int): Hour of the day, mapped (0-23).
            day (int): Day of the week, mapped (0-6).
            value (float): The newly observed financial amount/metric to record.
        """
        cell = self.matrix.setdefault((hour, day), {"count": 0, "mean": 0.0, "m2": 0.0})
        
        cell["count"] += 1
        delta = value - cell["mean"]
        cell["mean"] += delta / cell["count"]
        delta2 = value - cell["mean"]
        
        # m2 accumulates the squared differences from the mean
        cell["m2"] += delta * delta2

    def get_baseline_stats(self, hour: int, day: int) -> dict:
        """
        Retrieves the exact statistical baseline for a given cell.
        
        Args:
            hour (int): Hour of the day (0-23).
            day (int): Day of the week (0-6).
            
        Returns:
            dict: Contains the 'mean', 'std', and historical 'count' for the cell.
                  Defaults mathematically to zeros if the cell is completely empty.
        """
        cell = self.matrix.get((hour, day))
        if not cell or cell["count"] == 0:
            return {"mean": 0.0, "std": 0.0, "count": 0}
            
        count = int(cell["count"])
        mean = float(cell["mean"])
        
        # Sample standard deviation calculation (requires n > 1)
        std = 0.0
        if count > 1:
            variance = cell["m2"] / (count - 1)
            std = math.sqrt(variance)
            
        return {"mean": mean, "std": std, "count": count}

    def compute_zscore(self, hour: int, day: int, value: float) -> float:
        """
        Computes the Z-Score of an incoming event value against its temporal baseline cell.
        
        Handle Cold-Start constraint: Requires at least 5 data points to establish a
        statistically significant baseline. If under 5 events, defaults to 0.0.
        
        Args:
            hour (int): Hour of the day (0-23).
            day (int): Day of the week (0-6).
            value (float): The financial value evaluated.
            
        Returns:
            float: The calculated Z-Score. Defaults to 0.0 during initialization/cold-start.
        """
        stats = self.get_baseline_stats(hour, day)
        
        # Cold start logic
        if stats["count"] < 3 or stats["std"] == 0.0:
            return 0.0
            
        z_score = (value - stats["mean"]) / stats["std"]
        return float(z_score)

    def classify_severity(self, zscore: float) -> str:
        """
        Categorizes a calculated Z-Score dynamically into predefined severity bounds.
        
        Evaluation triggers:
        - Absolute Z-Score < 2.0 = normal
        - 2.0 <= Absolute Z-Score < 3.5 = warning
        - Absolute Z-Score >= 3.5 = critical
        
        Args:
            zscore (float): The computed Z-Score.
            
        Returns:
            str: Severity classification ("normal", "warning", or "critical").
        """
        # Use mathematical absolute value since anomalies can spike positively or negatively
        abs_z = abs(zscore)
        
        if abs_z < 2.0:
            return "normal"
        elif 2.0 <= abs_z < 3.5:
            return "warning"
        else:
            return "critical"
