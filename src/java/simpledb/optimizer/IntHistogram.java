package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 *
 * 这个优化是基于 扫描判断 最大值和最小值来进行的 判断要扫描的数量
 *
 * 直方图的扫描
 *
 * ？？？ 如何去得到一个数据构建这个直方图
 *
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private List<Integer> buckets;
    private int min;
    private int max;
    private double width;
    private int ntups;
    public IntHistogram(int buckets, int min, int max) {
        this.min = min;
        this.max = max;
        this.buckets = new ArrayList<>(buckets);
        ntups = 0;
        width = ((max - min + 1) * 1.0) / buckets;
        for (int i = 0; i < buckets; i ++) this.buckets.add(0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v < min || v > max) return;
        int t = buckets.get(index(v));
        buckets.set(index(v),  ++ t);
        ntups ++;
    }
    // 应该是向下取整
    private int index(int v) {
        return ((v - min) * buckets.size() / (max - min + 1));
    }
    /**其直方图的值
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        if (v < min)
            return (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ
            || op == Predicate.Op.NOT_EQUALS) ? 1.0 : 0.0;
        if (v > max)
            return (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ
             || op == Predicate.Op.NOT_EQUALS) ? 1.0 : 0.0;

        int i = index(v);
        int imax = (int) ((i + 1) * width + min);
        int cnt = 0;
        int imin = (int) (i * width + min);
        switch (op) {
            case EQUALS: return buckets.get(i) * 1.0 / ntups;
            case GREATER_THAN:
                for (int j = i + 1; j < buckets.size(); j ++)
                    cnt += buckets.get(j);
                return (buckets.get(i) * (imax - v) * 1.0 / width + cnt) / ntups;
            case GREATER_THAN_OR_EQ:
                for (int j = i; j < buckets.size(); j ++)
                    cnt += buckets.get(j);
                return cnt * 1.0 / ntups;
            case LESS_THAN:
                for (int j = 0; j < i; j ++)
                    cnt += buckets.get(j);
                return (cnt + buckets.get(i) * (v - imin) * 1.0 / width)/ ntups;
            case LESS_THAN_OR_EQ:
                for (int j = 0; j <= i; j ++)
                    cnt += buckets.get(j);
                return cnt * 1.0/ ntups;
            case NOT_EQUALS:
                return 1.0 - buckets.get(i) * 1.0 / ntups;
            default:
                return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
