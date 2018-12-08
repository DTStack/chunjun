package com.dtstack.flinkx.carbondata.writer;


public class TaskNumberGenerator {

    private TaskNumberGenerator() {

    }

    /**
     * Generate unique number to be used as partition number of file name
     */
    public static String generateUniqueNumber(int taskId, String segmentId, long partitionNumber) {
        return String.valueOf((int)(Math.pow(10,2)) + Integer.valueOf(segmentId))
                + String.valueOf((int)Math.pow(10,5) + taskId)
                + String.valueOf(partitionNumber + (int)Math.pow(10,5));
    }
}
