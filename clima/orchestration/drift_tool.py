from frouros.detectors.data_drift import KSTest
from frouros.metrics import PrequentialError

def drift_detector(new_data, old_data):
    detectors = []
    for column in old_data.columns:
        detector = KSTest()
        _ = detector.fit(X = data[column].values)
        detectors.append(detector)
    
    for column, detector in zip(column, detectors):
        p_value = detector.compare(X = old_data[column].values)[0].p_value
        if p_value <= 0.05:
            return True
    return False