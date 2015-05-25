package com.neverwinterdp.yara.quantile;

public interface IQuantileEstimator {
    void offer(long value);
    long getQuantile(double q);
}
