package cn.wangz.spark.telemetry.signal.profiles;

/** Opens dynamic profile labels on the current Spark task thread. */
public interface ProfileContext {
    ProfileContext NONE = new ProfileContext() {
        @Override public ProfileScope openStage(int stageId, int stageAttempt) {
            return ProfileScope.NONE;
        }
    };

    ProfileScope openStage(int stageId, int stageAttempt);
}
