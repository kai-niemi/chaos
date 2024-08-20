package io.roach.chaos.model;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("chaos")
public class Settings {
    private WorkloadType workloadType;

    private LockType lockType = LockType.NONE;

    private IsolationLevel isolationLevel = IsolationLevel.SERIALIZABLE;

    private Dialect dialect;

    private boolean retryJitter = true;

    private boolean export;

    private boolean quit;

    private boolean debugProxy;

    private boolean skipCreate;

    private boolean skipInit;

    private boolean skipRetry;

    private int contentionLevel = 8;

    private int numAccounts = 50_000;

    private int selection = 500;

    private int iterations = 1_000;

    private int workers;

    private String initFile;

    public boolean isQuit() {
        return quit;
    }

    public void setQuit(boolean quit) {
        this.quit = quit;
    }

    public String getInitFile() {
        return initFile;
    }

    public void setInitFile(String initFile) {
        this.initFile = initFile;
    }

    public int getContentionLevel() {
        return contentionLevel;
    }

    public void setContentionLevel(int contentionLevel) {
        this.contentionLevel = contentionLevel;
    }

    public boolean isDebugProxy() {
        return debugProxy;
    }

    public void setDebugProxy(boolean debugProxy) {
        this.debugProxy = debugProxy;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public boolean isExport() {
        return export;
    }

    public void setExport(boolean export) {
        this.export = export;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

    public int getNumAccounts() {
        return numAccounts;
    }

    public void setNumAccounts(int numAccounts) {
        this.numAccounts = numAccounts;
    }

    public boolean isRetryJitter() {
        return retryJitter;
    }

    public void setRetryJitter(boolean retryJitter) {
        this.retryJitter = retryJitter;
    }

    public int getSelection() {
        return selection;
    }

    public void setSelection(int selection) {
        this.selection = selection;
    }

    public boolean isSkipCreate() {
        return skipCreate;
    }

    public void setSkipCreate(boolean skipCreate) {
        this.skipCreate = skipCreate;
    }

    public boolean isSkipInit() {
        return skipInit;
    }

    public void setSkipInit(boolean skipInit) {
        this.skipInit = skipInit;
    }

    public boolean isSkipRetry() {
        return skipRetry;
    }

    public void setSkipRetry(boolean skipRetry) {
        this.skipRetry = skipRetry;
    }

    public int getWorkers() {
        return workers;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public WorkloadType getWorkloadType() {
        return workloadType;
    }

    public void setWorkloadType(WorkloadType workloadType) {
        this.workloadType = workloadType;
    }

    public boolean isOptimisticLocking() {
        return LockType.COMPARE_AND_SET.equals(lockType);
    }
}