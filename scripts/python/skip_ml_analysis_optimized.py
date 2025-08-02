import psycopg2
import pandas as pd
from db_config import db_params
from sklearn.preprocessing import LabelEncoder, StandardScaler
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, classification_report
import numpy as np
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'xgboost_training_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IncrementalMLTrainer:
    def __init__(self, db_params: Dict):
        """Initialize the trainer with database parameters and model configuration."""
        self.conn_string = f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"
        
        # Initialize XGBoost with careful parameters
        self.model = XGBClassifier(
            n_estimators=100,           # Start with fewer trees
            learning_rate=0.1,          # Moderate learning rate
            max_depth=8,                # Control tree depth
            min_child_weight=5,         # Helps prevent overfitting
            subsample=0.8,              # Use 80% of data per tree
            colsample_bytree=0.8,       # Use 80% of features per tree
            scale_pos_weight=5,         # Handle class imbalance
            tree_method='hist',         # Faster histogram-based algorithm
            random_state=42,
            n_jobs=-1                   # Use all CPU cores
        )
        
        # Initialize preprocessing components
        self.label_encoders = {
            'identity_pubkey': LabelEncoder(),
            'next_leader': LabelEncoder(),
            'city': LabelEncoder(),
            'country': LabelEncoder(),
            'continent': LabelEncoder()
        }
        self.scaler = StandardScaler()
        
        # Track unique values for categorical features
        self.unique_values = {col: set() for col in self.label_encoders.keys()}
        
        # Initialize model state
        self.is_first_batch = True
        self.feature_names = None
        self.trained_once = False  # New flag to track if model has been trained

    def get_latest_epoch(self):
        query = "SELECT MAX(epoch) FROM leader_schedule"
        with psycopg2.connect(self.conn_string) as conn:
            latest_epoch = pd.read_sql(query, conn).iat[0,0]
        return latest_epoch

    def get_epoch_data(self, epoch: int) -> Optional[pd.DataFrame]:
        """
        Fetch data for a single epoch along with features capturing
        skipped slots after a leader's 4th assigned slot.
        """
        # The subquery `leader_positions` calculates the leader slot positions mod 4
        # and identifies 4th leader slots.
        # Then we use a window function or a self-join to find subsequent skipped slots.
        epoch = int(epoch)  # ensure epoch is a Python int
        query = """
        WITH leader_positions AS (
            SELECT 
                ls.block_slot,
                ls.block_produced,
                ls.identity_pubkey,
                LEAD(ls.identity_pubkey) OVER (ORDER BY block_slot) AS next_leader,
                ((ls.block_slot - (%s * 432000)) % 4) AS position_in_group,
                COALESCE(vs.stake_percentage, 0) as stake_percentage,
                COALESCE(vs.skip_rate, 0) as skip_rate,
                COALESCE(vs.tx_included_in_blocks, 0) as tx_included_in_blocks,
                COALESCE(vs.votes_cast, 0) as votes_cast,
                COALESCE(vs.city, 'unknown') as city,
                COALESCE(vs.country, 'unknown') as country,
                COALESCE(vs.continent, 'unknown') as continent,
                COALESCE(vs.asn, -1) as asn,
                COALESCE(vs.skip_rate, 0) as previous_skip_rate
            FROM leader_schedule ls
            LEFT JOIN validator_stats vs
              ON ls.identity_pubkey = vs.identity_pubkey 
             AND ls.epoch = %s
            WHERE ls.epoch = %s
            ORDER BY ls.block_slot
        ),
        fourth_slots AS (
            SELECT
                lp.*,
                CASE WHEN position_in_group = 3 THEN 1 ELSE 0 END as is_4th_slot
            FROM leader_positions lp
        ),
        subsequent_skips AS (
            SELECT 
                fs.block_slot as fourth_slot,
                fs.identity_pubkey,
                fs.block_produced,
                fs.next_leader,
                fs.stake_percentage,
                fs.skip_rate,
                fs.tx_included_in_blocks,
                fs.votes_cast,
                fs.city,
                fs.country,
                fs.continent,
                fs.asn,
                fs.previous_skip_rate,
                fs.position_in_group,
                (SELECT COUNT(*) 
                 FROM leader_positions lp2 
                 WHERE lp2.block_slot > fs.block_slot
                   AND lp2.block_produced = False
                   AND lp2.identity_pubkey != fs.identity_pubkey
                   AND lp2.epoch = %s
                   -- Stop counting once we hit a produced block
                   AND lp2.block_slot < COALESCE(
                         (SELECT lp3.block_slot FROM leader_positions lp3 
                          WHERE lp3.block_slot > fs.block_slot 
                          AND lp3.block_produced = True
                          ORDER BY lp3.block_slot ASC LIMIT 1),
                         (SELECT MAX(block_slot) FROM leader_positions)
                       )
                ) as subsequent_skips_count,
                (SELECT COALESCE(
                           (SELECT lp3.block_slot - fs.block_slot
                            FROM leader_positions lp3
                            WHERE lp3.block_slot > fs.block_slot 
                              AND lp3.block_produced = True
                            ORDER BY lp3.block_slot ASC LIMIT 1),
                           0
                       )
                ) as subsequent_skips_duration
            FROM fourth_slots fs
        )
        SELECT
            ss.fourth_slot as block_slot,
            ss.block_produced,
            ss.identity_pubkey,
            ss.next_leader,
            ss.stake_percentage,
            ss.skip_rate,
            ss.tx_included_in_blocks,
            ss.votes_cast,
            ss.city,
            ss.country,
            ss.continent,
            ss.asn,
            ss.previous_skip_rate,
            ss.position_in_group,
            ss.subsequent_skips_count,
            ss.subsequent_skips_duration
        FROM subsequent_skips ss
        ORDER BY block_slot;
        """

        try:
            with psycopg2.connect(self.conn_string) as conn:
                logger.info(f"Fetching data for epoch {epoch}")
                df = pd.read_sql(query, conn, params=(epoch, epoch, epoch, epoch))
                logger.info(f"Retrieved {len(df)} rows for epoch {epoch}")
                return df
        except Exception as e:
            logger.error(f"Error fetching epoch {epoch}: {str(e)}")
            return None

    def update_categorical_encoders(self, data: pd.DataFrame) -> None:
        """Update label encoders with new categorical values."""
        for col in self.label_encoders.keys():
            new_values = set(data[col].unique())
            new_values_count = len(new_values - self.unique_values[col])
            
            if new_values_count > 0 or self.is_first_batch:
                # Refit encoder with combined values
                self.unique_values[col].update(new_values)
                self.label_encoders[col].fit(list(self.unique_values[col]))
                logger.info(f"Added {new_values_count} new values for {col}")

    def preprocess_data(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """Preprocess a batch of data, including new skip-based features."""
        # Update encoders with new categorical values
        self.update_categorical_encoders(data)
        
        # Transform categorical features
        encoded_data = {}
        for col, encoder in self.label_encoders.items():
            encoded_data[col] = encoder.transform(data[col])
        
        # Numerical features including new skip-based features
        numerical_cols = [
            'stake_percentage', 'skip_rate', 'tx_included_in_blocks',
            'votes_cast', 'asn', 'previous_skip_rate',
            'position_in_group', 'subsequent_skips_count', 'subsequent_skips_duration'
        ]

        # Fill NaN if any
        data[numerical_cols] = data[numerical_cols].fillna(0)

        # Scale numerical features
        if self.is_first_batch:
            numerical_data = self.scaler.fit_transform(data[numerical_cols])
        else:
            numerical_data = self.scaler.transform(data[numerical_cols])
        
        # Combine features
        feature_df = pd.DataFrame(encoded_data)
        feature_df[numerical_cols] = numerical_data
        
        if self.feature_names is None:
            self.feature_names = list(feature_df.columns)
        
        return feature_df.values, data['block_produced'].astype(int).values

    def train_epoch(self, epoch: int) -> Optional[float]:
        """Train the model on a single epoch."""
        data = self.get_epoch_data(epoch)
        if data is None or len(data) == 0:
            return None
        
        X, y = self.preprocess_data(data)
        
        try:
            if self.is_first_batch:
                logger.info("Training initial model...")
                self.model.fit(X, y)
                self.is_first_batch = False
                self.trained_once = True
            else:
                logger.info("Updating model with new data...")
                # Create a new model with the same parameters but initialized with the previous model
                new_model = XGBClassifier(**self.model.get_params())
                new_model.fit(X, y, xgb_model=self.model.get_booster())
                self.model = new_model
            
            # Calculate and return accuracy
            y_pred = self.model.predict(X)
            accuracy = accuracy_score(y, y_pred)
            logger.info(f"Epoch {epoch} - Accuracy: {accuracy:.4f}")
            
            # Log detailed classification report
            report = classification_report(y, y_pred)
            logger.info(f"Classification Report for epoch {epoch}:\n{report}")
            
            return accuracy
            
        except Exception as e:
            logger.error(f"Error training epoch {epoch}: {str(e)}")
            raise

    def save_model(self, filename: str):
        """Save the trained model and preprocessing components."""
        if not self.trained_once:
            logger.warning("Attempting to save untrained model")
            return
            
        model_data = {
            'model': self.model,
            'label_encoders': self.label_encoders,
            'scaler': self.scaler,
            'feature_names': self.feature_names,
            'unique_values': self.unique_values
        }
        import joblib
        joblib.dump(model_data, filename)
        logger.info(f"Model saved to {filename}")

def main():
    # Initialize trainer
    trainer = IncrementalMLTrainer(db_params)
    
    # Determine latest epoch
    latest_epoch = trainer.get_latest_epoch()
    # Train on epochs 648 to latest - 1
    # Test on epoch = latest_epoch
    train_start_epoch = 648
    train_end_epoch = latest_epoch - 1

    # Training loop
    accuracies = {}
    try:
        for epoch in range(train_start_epoch, train_end_epoch + 1):
            logger.info(f"Processing epoch {epoch}")
            accuracy = trainer.train_epoch(epoch)
            if accuracy is not None:
                accuracies[epoch] = accuracy
            
            # Save intermediate results periodically
            if epoch % 10 == 0 and trainer.trained_once:
                trainer.save_model(f'xgboost_model_epoch_{epoch}.joblib')
                with open(f'accuracies_up_to_epoch_{epoch}.json', 'w') as f:
                    json.dump(accuracies, f, indent=2)
        
        # After training completes, test on the latest epoch
        logger.info(f"Testing on latest epoch {latest_epoch}")
        test_data = trainer.get_epoch_data(latest_epoch)
        if test_data is not None and len(test_data) > 0:
            X_test, y_test = trainer.preprocess_data(test_data)
            y_pred_test = trainer.model.predict(X_test)
            test_accuracy = accuracy_score(y_test, y_pred_test)
            logger.info(f"Test Epoch {latest_epoch} - Accuracy: {test_accuracy:.4f}")
            test_report = classification_report(y_test, y_pred_test)
            logger.info(f"Classification Report for test epoch {latest_epoch}:\n{test_report}")

    except KeyboardInterrupt:
        logger.info("Training interrupted by user")
    except Exception as e:
        logger.error(f"Error during training: {str(e)}", exc_info=True)
    finally:
        # Save final model and results
        if trainer.trained_once:
            trainer.save_model('xgboost_model_final.joblib')
            with open('final_accuracies.json', 'w') as f:
                json.dump(accuracies, f, indent=2)
        
        logger.info("Training complete")
        logger.info(f"Final number of epochs processed: {len(accuracies)}")
        if accuracies:
            logger.info(f"Average training accuracy: {np.mean(list(accuracies.values())):.4f}")

if __name__ == "__main__":
    main()
