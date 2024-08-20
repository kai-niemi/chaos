package io.roach.chaos.repository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.roach.chaos.model.AccountType;
import io.roach.chaos.util.RandomData;

public class CockroachAccountRepository extends AbstractAccountRepository {
    protected String generateSeriesSQL() {
        return "select unordered_unique_rowid() FROM generate_series(1, ?) AS i";
    }

    @Override
    public void createAccounts(BigDecimal initialBalance,
                               int count,
                               Consumer<Integer> progress) {
        jdbcTemplate.execute("TRUNCATE table account");

        Collection<List<Integer>> result = IntStream.rangeClosed(1, count / 2).boxed()
                .collect(Collectors.groupingBy(it -> it / BATCH_SIZE))
                .values();

        for (List<Integer> batch : result) {
            List<Long> generatedIds = jdbcTemplate.query(generateSeriesSQL(),
                    (rs, rowNum) -> rs.getLong(1),
                    batch.size()
            );

            jdbcTemplate.update(
                    "INSERT INTO account(id,balance,name,type)"
                            + " select"
                            + "  unnest(?) as id,"
                            + "  unnest(?) as balance,"
                            + "  unnest(?) as name,"
                            + "  unnest(?) as type"
                            + " ON CONFLICT (id,type) do nothing",
                    ps -> {
                        List<Long> ids = new ArrayList<>();
                        List<BigDecimal> balances = new ArrayList<>();
                        List<String> names = new ArrayList<>();
                        List<String> types = new ArrayList<>();

                        generatedIds.forEach(id -> {
                            ids.add(id);
                            balances.add(initialBalance);
                            names.add(RandomData.randomString(32));
                            types.add(AccountType.checking.name());

                            ids.add(id);
                            balances.add(initialBalance);
                            names.add(RandomData.randomString(32));
                            types.add(AccountType.credit.name());
                        });

                        ps.setArray(1, ps.getConnection().createArrayOf("LONG", ids.toArray()));
                        ps.setArray(2, ps.getConnection().createArrayOf("DECIMAL", balances.toArray()));
                        ps.setArray(3, ps.getConnection().createArrayOf("VARCHAR", names.toArray()));
                        ps.setArray(4, ps.getConnection().createArrayOf("VARCHAR", types.toArray()));

                        ps.executeLargeUpdate();

                        progress.accept(generatedIds.size() * 2);
                    });
        }
    }

}
