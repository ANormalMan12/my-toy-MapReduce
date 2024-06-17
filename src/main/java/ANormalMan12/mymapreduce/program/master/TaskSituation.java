package ANormalMan12.mymapreduce.program.master;

import java.io.Serializable;

public enum TaskSituation implements Serializable {
    submitted, running, done
}