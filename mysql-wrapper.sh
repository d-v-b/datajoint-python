#!/bin/bash
# MySQL wrapper script for clean isolated startup

# Use project-specific directory that gets cleaned properly
MYSQL_DIR="$PIXI_PROJECT_ROOT/.mysql-data"
MYSQL_SOCKET="$MYSQL_DIR/mysql.sock"
MYSQL_PID="$MYSQL_DIR/mysql.pid"

# Cleanup function - more aggressive cleanup
cleanup_mysql() {
    echo "Cleaning up MySQL processes and data..."
    
    # Kill any running MySQL processes
    if [ -f "$MYSQL_PID" ]; then
        kill "$(cat "$MYSQL_PID")" 2>/dev/null || true
        rm -f "$MYSQL_PID"
    fi
    
    # Kill any mysqld processes that might be running
    pkill -f mysqld 2>/dev/null || true
    sleep 2
    
    # Remove entire MySQL directory to avoid any file conflicts
    rm -rf "$MYSQL_DIR"
    
    # Clean up any leftover MySQL files in project root
    rm -f "$PIXI_PROJECT_ROOT"/*.sock* 2>/dev/null || true
    rm -f "$PIXI_PROJECT_ROOT"/mysql.pid* 2>/dev/null || true
}

# Initialize MySQL with clean state
init_mysql() {
    echo "Initializing fresh MySQL instance..."
    cleanup_mysql
    
    # Create fresh data directory
    mkdir -p "$MYSQL_DIR"
    
    # Initialize with consistent settings
    mysqld --initialize-insecure \
        --datadir="$MYSQL_DIR" \
        --user="$(whoami)" \
        --log-error="$MYSQL_DIR/init.log"
        
    if [ $? -eq 0 ]; then
        echo "MySQL initialized successfully"
    else
        echo "MySQL initialization failed. Check $MYSQL_DIR/init.log"
        cat "$MYSQL_DIR/init.log" 2>/dev/null || true
        return 1
    fi
}

# Start MySQL
start_mysql() {
    echo "Starting MySQL server..."
    
    # Check if MySQL is already initialized (look for ibdata1 instead of user.frm for MySQL 8.0)
    if [ ! -f "$MYSQL_DIR/ibdata1" ]; then
        echo "MySQL not initialized, initializing first..."
        init_mysql || return 1
    fi
    
    # Start MySQL server using configuration file
    mysqld \
        --defaults-file="$PIXI_PROJECT_ROOT/mysql.cnf" \
        --user="$(whoami)" &
    
    # Wait for MySQL to be ready
    for i in {1..30}; do
        if mysqladmin ping --socket="$MYSQL_SOCKET" --silent 2>/dev/null; then
            echo "MySQL started successfully"
            return 0
        fi
        sleep 1
    done
    
    echo "MySQL failed to start"
    return 1
}

# Setup users
setup_users() {
    echo "Setting up MySQL users..."
    mysql --socket="$MYSQL_SOCKET" -u root -e "
        ALTER USER 'root'@'localhost' IDENTIFIED BY 'password';
        CREATE USER IF NOT EXISTS 'datajoint'@'%' IDENTIFIED BY 'datajoint';
        GRANT ALL PRIVILEGES ON *.* TO 'datajoint'@'%' WITH GRANT OPTION;
        FLUSH PRIVILEGES;
    " 2>/dev/null || echo "User setup completed"
}

case "$1" in
    "init")
        init_mysql
        ;;
    "start")
        start_mysql && setup_users
        ;;
    "stop")
        if [ -f "$MYSQL_PID" ]; then
            echo "Stopping MySQL..."
            kill "$(cat "$MYSQL_PID")" 2>/dev/null || true
            rm -f "$MYSQL_PID"
            echo "MySQL stopped"
        fi
        ;;
    "clean")
        cleanup_mysql
        ;;
    *)
        echo "Usage: $0 {init|start|stop|clean}"
        exit 1
        ;;
esac