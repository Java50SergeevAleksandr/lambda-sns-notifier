package telran.aws.probes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

import org.json.simple.parser.JSONParser;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;

import telran.probes.dto.DeviationData;

public class SnsNotifierHandler implements RequestHandler<KinesisEvent, String> {
	String topicBase;
	String subjectBase;
	String awsRegion;
	AmazonSNS client;
	LambdaLogger log;

	public SnsNotifierHandler() {
		topicBase = System.getenv("TOPIC_BASE");
		subjectBase = System.getenv("SUBJECT_BASE");
		awsRegion = System.getenv("REGION");
		client = AmazonSNSClient.builder().withRegion(awsRegion).build();
	}

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		log = context.getLogger();
		log.log("topicBase is " + topicBase);
		log.log("subjectBase is " + subjectBase);
		log.log("awsRegion is " + awsRegion);
		try {
			input.getRecords().stream().map(this::toDeviationData).forEach(this::publishingNotification);
		} catch (Exception e) {
			log.log("ERROR: " + e);
		}
		return null;
	}

	void publishingNotification(DeviationData deviation) {
		long sensorId = deviation.id();
		client.publish(topicBase + sensorId, getMessage(deviation), subjectBase + sensorId);
	}

	private String getMessage(DeviationData deviationData) {
		String text = String.format("sensor %d has value %f with deviation %f at %s", deviationData.id(),
				deviationData.value(), deviationData.deviation(), getDateTime(deviationData.timestamp()));
		return text;
	}

	private LocalDateTime getDateTime(long timestamp) {
		return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
	}

	DeviationData toDeviationData(KinesisEventRecord record) {
		String recordStr = new String(record.getKinesis().getData().array());
		log.log("received string:  " + recordStr);
		int index = recordStr.indexOf('{');
		if (index < 0) {
			throw new RuntimeException("not found JSON");
		}
		String deviationDataJSON = recordStr.substring(index);
		DeviationData dataResult = getDeviationData(deviationDataJSON);
		log.log("Deviation data " + dataResult);
		return dataResult;

	}

	@SuppressWarnings("unchecked")
	private DeviationData getDeviationData(String deviationDataJSON) {
		JSONParser parser = new JSONParser();
		DeviationData result = null;
		try {
			Map<String, Object> map = (Map<String, Object>) parser.parse(deviationDataJSON);
			result = new DeviationData((long) map.get("id"), (double) map.get("deviation"), (double) map.get("value"),
					(long) map.get("timestamp"));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		return result;
	}

}