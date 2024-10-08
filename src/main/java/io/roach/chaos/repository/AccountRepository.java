package io.roach.chaos.repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.data.util.Pair;

import io.roach.chaos.model.Account;
import io.roach.chaos.model.AccountType;
import io.roach.chaos.model.LockType;

public interface AccountRepository {
    String databaseVersion();

    String isolationLevel();

    void createAccounts(BigDecimal initialBalance,
                        int count,
                        Consumer<Integer> progress);

    void createAccount(Account account);

    void deleteAccount(Account.Id id);

    Account findAccountById(Account.Id id, LockType lock);

    List<Account> findAccountsById(Long id, LockType lock);

    List<Account> findTargetAccounts(int limit, boolean random);

    void updateBalance(Account account);

    void updateBalanceCAS(Account account);

    void addBalance(Long id,
                    AccountType type,
                    BigDecimal amount);

    void addBalanceCAS(Long id,
                       AccountType type,
                       BigDecimal amount,
                       Integer version);

    BigDecimal totalAccountBalance(Long id);

    void findNegativeBalances(Consumer<Pair<Long, BigDecimal>> consumer);

    BigDecimal sumTotalBalance();
}
