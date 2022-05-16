package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.redis.lettucemod.gears.Registration;

public class RegistrationListOutput<K, V> extends CommandOutput<K, V, List<Registration>>
		implements StreamingOutput<Registration> {

	private static final String FIELD_NUM_TRIGGERED = "numTriggered";
	private static final String FIELD_ID = "id";
	private static final String FIELD_READER = "reader";
	private static final String FIELD_DESC = "desc";
	private static final String FIELD_REGISTRATION_DATA = "RegistrationData";
	private static final String FIELD_MODE = "mode";
	private static final String FIELD_LAST_ERROR = "lastError";
	private static final String FIELD_ARGS = "args";
	private static final String FIELD_STATUS = "status";
	private static final String FIELD_PD = "PD";
	private static final String FIELD_NUM_SUCCESS = "numSuccess";
	private static final String FIELD_NUM_FAILURES = "numFailures";
	private static final String FIELD_NUM_ABORTED = "numAborted";
	private static final String FIELD_LAST_RUN_DURATION = "lastRunDurationMS";
	private static final String FIELD_TOTAL_RUN_DURATION = "totalRunDurationMS";
	private static final String FIELD_AVG_RUN_DURATION = "avgRunDurationMS";
	private static final String FIELD_LAST_ESTIMATED_LAG = "lastEstimatedLagMS";
	private static final String FIELD_AVG_ESTIMATED_LAG = "avgEstimatedLagMS";

	private boolean initialized;
	private Subscriber<Registration> subscriber;
	private Registration registration;
	private String field;
	private int argSize;

	public RegistrationListOutput(RedisCodec<K, V> codec) {
		super(codec, Collections.emptyList());
		setSubscriber(ListSubscriber.instance());
	}

	@Override
	public void set(ByteBuffer bytes) {
		if (field == null) {
			field = decodeAscii(bytes);
			return;
		}
		if (FIELD_ID.equals(field)) {
			registration = new Registration();
			registration.setId(decodeAscii(bytes));
			field = null;
			return;
		}
		if (FIELD_READER.equals(field)) {
			registration.setReader(decodeAscii(bytes));
			field = null;
			return;
		}
		if (FIELD_DESC.equals(field)) {
			registration.setDescription(decodeAscii(bytes));
			field = null;
			return;
		}
		if (FIELD_REGISTRATION_DATA.equals(field)) {
			registration.setData(new Registration.Data());
			field = decodeAscii(bytes);
			return;
		}
		if (registration != null) {
			if (registration.getData() != null) {
				if (FIELD_MODE.equals(field)) {
					registration.getData().setMode(decodeAscii(bytes));
					field = null;
					return;
				}
				if (FIELD_LAST_ERROR.equals(field)) {
					registration.getData().setLastError(decodeAscii(bytes));
					field = null;
					return;
				}
				if (FIELD_ARGS.equals(field)) {
					field = decodeAscii(bytes);
					return;
				}
				if (registration.getData().getArgs() != null && registration.getData().getArgs().size() < argSize) {
					registration.getData().getArgs().put(field, decodeAscii(bytes));
					field = null;
					return;
				}
				if (FIELD_STATUS.equals(field)) {
					registration.getData().setStatus(decodeAscii(bytes));
					field = null;
					return;
				}
			}
			if (FIELD_PD.equals(field)) {
				registration.setPrivateData(decodeAscii(bytes));
				field = null;
				return;
			}
			subscriber.onNext(output, registration);
			registration = null;
			return;
		}
	}

	@Override
	public void set(long integer) {
		if (registration != null && registration.getData() != null) {
			if (FIELD_NUM_TRIGGERED.equals(field)) {
				registration.getData().setNumTriggered(integer);
				field = null;
				return;
			}
			if (FIELD_NUM_SUCCESS.equals(field)) {
				registration.getData().setNumSuccess(integer);
				field = null;
				return;
			}
			if (FIELD_NUM_FAILURES.equals(field)) {
				registration.getData().setNumFailures(integer);
				field = null;
				return;
			}
			if (FIELD_NUM_ABORTED.equals(field)) {
				registration.getData().setNumAborted(integer);
				field = null;
				return;
			}
			if (FIELD_LAST_RUN_DURATION.equals(field)) {
				registration.getData().setLastRunDurationMS(integer);
				field = null;
				return;
			}
			if (FIELD_TOTAL_RUN_DURATION.equals(field)) {
				registration.getData().setLastRunDurationMS(integer);
				field = null;
				return;
			}
			if (FIELD_AVG_RUN_DURATION.equals(field)) {
				registration.getData().setAvgRunDurationMS(integer);
				field = null;
				return;
			}
			if (FIELD_LAST_ESTIMATED_LAG.equals(field)) {
				registration.getData().setLastEstimatedLagMS(integer);
				field = null;
				return;
			}
			if (FIELD_AVG_ESTIMATED_LAG.equals(field)) {
				registration.getData().setAvgEstimatedLagMS(integer);
				field = null;
				return;
			}
			if (registration.getData().getArgs() != null && registration.getData().getArgs().size() < argSize) {
				registration.getData().getArgs().put(field, integer);
				field = null;
				return;
			}
		}
	}

	@Override
	public void set(double number) {
		if (registration != null && registration.getData() != null) {
			if (FIELD_LAST_RUN_DURATION.equals(field)) {
				registration.getData().setLastRunDurationMS(Math.round(number));
				field = null;
				return;
			}
			if (FIELD_TOTAL_RUN_DURATION.equals(field)) {
				registration.getData().setLastRunDurationMS(Math.round(number));
				field = null;
				return;
			}
			if (FIELD_AVG_RUN_DURATION.equals(field)) {
				registration.getData().setAvgRunDurationMS(Math.round(number));
				field = null;
				return;
			}
			if (FIELD_LAST_ESTIMATED_LAG.equals(field)) {
				registration.getData().setLastEstimatedLagMS(Math.round(number));
				field = null;
				return;
			}
			if (FIELD_AVG_ESTIMATED_LAG.equals(field)) {
				registration.getData().setAvgEstimatedLagMS(Math.round(number));
				field = null;
				return;
			}
			if (registration.getData().getArgs() != null && registration.getData().getArgs().size() < argSize) {
				registration.getData().getArgs().put(field, number);
				field = null;
				return;
			}
		}
	}

	@Override
	public void multi(int count) {
		if (!initialized) {
			output = OutputFactory.newList(count);
			initialized = true;
			return;
		}
		if (FIELD_ARGS.equals(field) && registration != null && registration.getData() != null) {
			argSize = count / 2;
			registration.getData().setArgs(new HashMap<>(argSize));
		}
	}

	public void setSubscriber(Subscriber<Registration> subscriber) {
		this.subscriber = subscriber;
	}

	@Override
	public Subscriber<Registration> getSubscriber() {
		return subscriber;
	}

}
