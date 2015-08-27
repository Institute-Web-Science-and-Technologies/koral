package de.uni_koblenz.west.cidre.common.networManager;

import org.zeromq.ZContext;

public class NetworkContextFactory {

	private static ZContext mainContext;

	private static int numberOfShadowedContexts;

	public static ZContext getNetworkContext() {
		if (mainContext == null) {
			mainContext = new ZContext();
			numberOfShadowedContexts = 0;
		}
		numberOfShadowedContexts++;
		return ZContext.shadow(mainContext);
	}

	public static void destroyNetworkContext(ZContext context) {
		context.destroy();
		numberOfShadowedContexts--;
		if (numberOfShadowedContexts == 0) {
			mainContext.destroy();
			mainContext = null;
		}
	}

}
