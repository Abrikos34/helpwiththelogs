package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private static final Logger logger = LoggerFactory.getLogger(UserDaoJDBCImpl.class);

    public UserDaoJDBCImpl() {
    }

    /**
     * Создание таблицы пользователей.
     */
    @Override
    public void createUsersTable() {
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                "name VARCHAR(255) NOT NULL, " +
                "lastName VARCHAR(255) NOT NULL, " +
                "age TINYINT NOT NULL" +
                ")";
        try (Connection conn = Util.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            logger.info("Таблица 'users' успешно создана или уже существует.");
        } catch (SQLException e) {
            logger.error("Ошибка при создании таблицы 'users': {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Удаление таблицы пользователей.
     */
    @Override
    public void dropUsersTable() {
        String sql = "DROP TABLE IF EXISTS users";
        try (Connection conn = Util.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            logger.info("Таблица 'users' успешно удалена.");
        } catch (SQLException e) {
            logger.error("Ошибка при удалении таблицы 'users': {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Сохранение пользователя в таблицу.
     */
    @Override
    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";
        try (Connection conn = Util.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setString(2, lastName);
            pstmt.setByte(3, age);
            pstmt.executeUpdate();
            logger.info("Пользователь '{}' {} (возраст: {}) добавлен в базу данных.", name, lastName, age);
        } catch (SQLException e) {
            logger.error("Ошибка при добавлении пользователя: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Удаление пользователя по ID.
     */
    @Override
    public void removeUserById(long id) {
        String sql = "DELETE FROM users WHERE id = ?";
        try (Connection conn = Util.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                logger.info("Пользователь с ID {} успешно удалён.", id);
            } else {
                logger.warn("Пользователь с ID {} не найден в базе данных.", id);
            }
        } catch (SQLException e) {
            logger.error("Ошибка при удалении пользователя с ID {}: {}", id, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Получение всех пользователей.
     */
    @Override
    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String sql = "SELECT * FROM users";
        try (Connection conn = Util.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                User user = new User();
                user.setId(rs.getLong("id"));
                user.setName(rs.getString("name"));
                user.setLastName(rs.getString("lastName"));
                user.setAge(rs.getByte("age"));
                users.add(user);
            }
            logger.info("Получено {} пользователей из базы данных.", users.size());
        } catch (SQLException e) {
            logger.error("Ошибка при получении списка пользователей: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        return users;
    }

    /**
     * Очистка таблицы пользователей.
     */
    @Override
    public void cleanUsersTable() {
        String sql = "TRUNCATE TABLE users";
        try (Connection conn = Util.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int affectedRows = pstmt.executeUpdate();
            logger.info("Таблица 'users' успешно очищена. Удалено строк: {}", affectedRows);
        } catch (SQLException e) {
            logger.error("Ошибка при очистке таблицы 'users': {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}

