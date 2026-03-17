package org.bds.wsh.tests;

public final class TestMain {
    private TestMain() {
    }

    public static void main(String[] args) throws Exception {
        new WorkflowTests().run();
        new SchedulerTests().run();
        new MetricsTests().run();
        new DaxLoaderTests().run();
        new TrainingProfileCsvTests().run();
        System.out.println("All Java scheduler tests passed.");
    }
}
