package io.odysz.semantic.DA.drvmnger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * An {@link ArrayBlockingQueue}'s test version.
 * 
 * @since 1.4.45
 * @param <S>
 */
public class T_ArrayBlockingQueue<S> extends ArrayBlockingQueue<S> {

	static List<Integer> qusizeOnTaking;

	private static final long serialVersionUID = 1L;

	public T_ArrayBlockingQueue(int capacity) {
		super(capacity);
		if (qusizeOnTaking == null)
			qusizeOnTaking = new ArrayList<Integer>();
	}

	@Override
	public S take() throws InterruptedException {
		qusizeOnTaking.add(size());
		return super.take();
	}
}
