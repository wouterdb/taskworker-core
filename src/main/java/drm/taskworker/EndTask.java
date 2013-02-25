package drm.taskworker;

/**
 * A task used to signal that this is the end of a workflow
 * 
 * @author bart
 *
 */
public class EndTask extends Task {
	/**
	 * Signal the end of a workflow to the next step in the workflow 
	 * 
	 * @param nextStepName The name of the next step.
	 */
	public EndTask(String nextStepName) {
		super(nextStepName);
	}
}
