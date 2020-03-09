# Create function to update existing user updated timestamp
CREATE OR REPLACE FUNCTION prediction_changed() RETURNS trigger 
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at := current_timestamp;
  RAISE NOTICE 'Prediction changed for user ''%'' on %', OLD.user_id, NEW.updated_at;
  RETURN NEW;
END;
$$;



# Create trigger to execute the update timestamp function when new prediction score is different with the old one
CREATE TRIGGER trigger_prediction_changed
  BEFORE UPDATE ON referral_score
  FOR EACH ROW
  WHEN (OLD.prediction_n IS DISTINCT FROM NEW.prediction_n)
  EXECUTE PROCEDURE prediction_changed();