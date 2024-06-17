package ANormalMan12.mymapreduce.program.worker;

import java.util.UUID;

abstract  public interface WorkerExeClass {
    abstract public void execute(UUID jobId,UUID taskTrackerId, int indexNumber) throws Exception;
}
