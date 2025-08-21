class AddS3TestCasesToSubmissions < ActiveRecord::Migration[6.1]
  def change
    add_column :submissions, :s3_test_cases, :text
  end
end
