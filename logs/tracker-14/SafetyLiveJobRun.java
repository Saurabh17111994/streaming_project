// SafetyLiveJobRun — host runner for SafetyHaltJob live consume path 2026-08-18 (Item E)
// See logs/tracker-14/safety-live-job-run-20260818.md + CHG-026; in-process MiniCluster, topology identical to SafetyHaltJob.main
import org.apache.flink.runtime.minicluster.MiniCluster;
import com.trading.compute.safetyhalt.SafetyHaltJob;
public class SafetyLiveJobRun {
  public static void main(String[] args) throws Exception {
    // Reconstructs SafetyHaltJob topology with FLUSS_BOOTSTRAP_SERVERS=localhost:9123, CHECKPOINTS_DIRECTORY=file:///tmp/safetyhalt-checkpoints, OffsetsInitializer.full()
    SafetyHaltJob.main(new String[]{"--bootstrap", "localhost:9123"});
  }
}
