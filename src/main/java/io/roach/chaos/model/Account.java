package io.roach.chaos.model;

import java.math.BigDecimal;
import java.util.Objects;

public class Account {
    private Id id;

    private Integer version;

    private BigDecimal balance;

    public Id getId() {
        return id;
    }

    public Account setId(Id id) {
        this.id = id;
        return this;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public Account setBalance(BigDecimal balance) {
        this.balance = balance;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public Account setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public Account addBalance(BigDecimal delta) {
        this.balance = balance.add(delta);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Account account = (Account) o;
        return Objects.equals(id, account.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "Account{" +
                "id=" + id +
                ", version=" + version +
                ", balance=" + balance +
                '}';
    }

    public static class Id {
        private final Long id;

        private final AccountType type;

        public Id(Long id, String type) {
            this(id, AccountType.valueOf(type));
        }

        public Id(Long id, AccountType type) {
            this.id = id;
            this.type = type;
        }

        public Long getId() {
            return id;
        }

        public AccountType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Id id1 = (Id) o;

            if (!id.equals(id1.id)) {
                return false;
            }
            return type == id1.type;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Id{" +
                    "id=" + id +
                    ", type=" + type +
                    '}';
        }
    }
}
