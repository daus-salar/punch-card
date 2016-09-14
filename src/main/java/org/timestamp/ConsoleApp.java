package org.timestamp;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;

public class ConsoleApp {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    private static final Logger LOGGER = LogManager.getLogger(ConsoleApp.class);

    public static void main(String[] args) throws Exception {
	new ConsoleApp().run();
    }

    public void run() throws Exception {
	Files.find(Paths.get("."), 1, endsOn(".csv")).map(path -> new Tuple2<>(path, processTimestamps(path)))

		.forEach(t2 -> {
		    try {
			System.out.println("=================================");
			Path path = t2.f0;
			String filePath = path.getParent().resolve("output").resolve("stat_" + path.getFileName())
				.toUri().toString();
			System.out.println(filePath);
			t2.f1.writeAsCsv(filePath, WriteMode.OVERWRITE);
			System.out.println("=================================");
		    } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		});
	env.setParallelism(1);
	env.execute();
    }

    static class TimeStamp {
	private final LocalDateTime time;
	private final String level;
	private final String source;
	private final Long cause;
	private final String unkown;

	public TimeStamp(Tuple5<String, String, String, Long, String> value) {
	    level = value.f0;
	    time = parseTimeStamp(value);
	    source = value.f2;
	    cause = value.f3;
	    unkown = value.f4;

	}

	private LocalDateTime parseTimeStamp(Tuple5<String, String, String, Long, String> value) {
	    return DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss").parse(value.f1).query(LocalDateTime::from);
	}

    }

    private DataSet<?> processTimestamps(Path path) {
	LOGGER.info("Process {}", () -> path.getFileName());
	// defining a series
	DataSource<Tuple5<String, String, String, Long, String>> csvData = env.readCsvFile(path.toString())
		.ignoreComments("#").fieldDelimiter(";").ignoreInvalidLines()
		.types(String.class, String.class, String.class, Long.class, String.class);

	UnsortedGrouping<TimeStamp> groupedByDate = csvData.map(value -> new TimeStamp(value)).returns(TimeStamp.class)
		.groupBy(value -> value.time.toLocalDate());

	GroupReduceOperator<TimeStamp, Range<LocalDateTime>> reduceGroup = groupedByDate
		.reduceGroup(reduceTimestampsToTimeRange()).returns(new TypeHint<Range<LocalDateTime>>() {
		});
	SortPartitionOperator<Range<LocalDateTime>> sortedPartition = reduceGroup.sortPartition(lowerEndPointSelector(),
		Order.ASCENDING);
	return sortedPartition.map(outputFormat());

    }

    private MapFunction<Range<LocalDateTime>, Tuple3<LocalDate, Range<LocalTime>, Duration>> outputFormat() {
	return new MapFunction<Range<LocalDateTime>, Tuple3<LocalDate, Range<LocalTime>, Duration>>() {

	    @Override
	    public Tuple3<LocalDate, Range<LocalTime>, Duration> map(Range<LocalDateTime> value) throws Exception {
		LocalDate date = LocalDate.from(value.lowerEndpoint());

		Range<LocalTime> newRange = Range.closed(LocalTime.from(value.lowerEndpoint()),
			LocalTime.from(value.upperEndpoint()));
		return new Tuple3<>(date, newRange, Duration.between(value.lowerEndpoint(), value.upperEndpoint()));
	    }

	};
    }

    private KeySelector<Range<LocalDateTime>, LocalDateTime> lowerEndPointSelector() {
	return new KeySelector<Range<LocalDateTime>, LocalDateTime>() {
	    @Override
	    public LocalDateTime getKey(Range<LocalDateTime> value) throws Exception {
		return value.lowerEndpoint();
	    }
	};
    }

    private static GroupReduceFunction<TimeStamp, Range<LocalDateTime>> reduceTimestampsToTimeRange() {
	return (values, out) -> {
	    Spliterator<TimeStamp> spliterator = values.spliterator();
	    Optional<Range<LocalDateTime>> identity = first(spliterator).map(ts1 -> ts1.time)
		    .map(ldt -> Range.singleton(ldt));
	    if (identity.isPresent()) {
		out.collect(StreamSupport.stream(spliterator, false).reduce(identity.get(),
			(r, ts) -> r.span(Range.singleton(ts.time)), Range::span));
	    }
	};

    }

    private static <T> Optional<T> first(Spliterator<T> iterator) {
	AtomicReference<T> ref = new AtomicReference<>();
	iterator.tryAdvance(first -> ref.set(first));
	return Optional.ofNullable(ref.get());
    }

    private static BiPredicate<Path, BasicFileAttributes> endsOn(String fileExtension) {
	return (path, u) -> path.toString().endsWith(fileExtension);
    }
}
